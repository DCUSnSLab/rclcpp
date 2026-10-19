// Copyright 2026 nev
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
// writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#include "rclcpp/experimental/executors/rcl_executor.hpp"
#include <deque>
#include <exception>
#include <functional>
#include <limits>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "rcl_executor/executor.h"
#include <rclcpp/exceptions.hpp>
#include <rclcpp/experimental/subscription_intra_process_base.hpp>
#include <rcpputils/scope_exit.hpp>

namespace rclcpp::experimental::executors
{
namespace
{
void checked(rcl_ret_t result)
{
  if (result != RCL_RET_OK) {rclcpp::exceptions::throw_from_rcl_error(result);}
}
}  // namespace

struct RclExecutor::Impl
{
  struct Entity
  {
    uint64_t id = 0;
    CallbackGroup::WeakPtr group;
    std::shared_ptr<const void> handle;
    Clock::SharedPtr clock;
    std::function<bool(const rcl_executor_item_t &)> execute;
    std::function<void()> clear;
    std::mutex mutex;
    std::deque<int> events;
    bool guard_only = false;
  };
  RclExecutor & owner;
  rcl_executor_t * core = nullptr;
  rcl_executor_handle_t * handle = nullptr;
  uint64_t maintenance = 0;
  size_t threads;
  std::mutex mutex;
  std::unordered_map<const void *, std::shared_ptr<Entity>> entities;
  std::exception_ptr error;

  Impl(RclExecutor & executor, size_t count)
  : owner(executor), threads(std::max<size_t>(count, 1))
  {
    checked(rcl_executor_create(&core));
    handle = rcl_executor_get_handle(core);
    checked(rcl_executor_add_group(core, UINT64_MAX, RCL_EXECUTOR_MUTUALLY_EXCLUSIVE));
    checked(rcl_executor_add(core, RCL_EXECUTOR_WORK, nullptr, nullptr, UINT64_MAX, &maintenance));
    owner.notify_waitable_->set_on_ready_callback([this](size_t, int) {
        auto result = rcl_executor_notify(core, maintenance, 1);
        if (result != RCL_RET_OK) {rcl_reset_error();}
      });
  }
  ~Impl()
  {
    owner.notify_waitable_->clear_on_ready_callback();
    for (auto & pair : entities) {
      if (pair.second->clear) {pair.second->clear();}
      checked(rcl_executor_remove(core, pair.second->id));
    }
    rcl_executor_destroy(core);
    rcl_executor_handle_destroy(handle);
  }

  void sync()
  {
    std::lock_guard<std::mutex> lock(mutex);
    std::unordered_set<const void *> seen;
    for (auto & weak : owner.get_all_callback_groups()) {
      auto group = weak.lock();
      if (!group) {continue;}
      auto group_id = reinterpret_cast<uintptr_t>(group.get());
      checked(rcl_executor_add_group(core, group_id,
          group->type() == CallbackGroupType::Reentrant ?
          RCL_EXECUTOR_REENTRANT : RCL_EXECUTOR_MUTUALLY_EXCLUSIVE));
      auto add = [&](const auto & pointer, int type, auto native_handle, auto execute) {
          seen.insert(pointer.get());
          if (entities.count(pointer.get())) {return;}
          auto entity = std::make_shared<Entity>();
          entity->group = group;
          entity->handle = native_handle;
          entity->execute = execute;
          checked(rcl_executor_add(core, static_cast<rcl_executor_entity_type_t>(type),
              native_handle.get(), pointer.get(), group_id, &entity->id));
          entities.emplace(pointer.get(), entity);
        };
      std::function<void(const Waitable::SharedPtr &)> add_waitable;
      add_waitable = [&](const Waitable::SharedPtr & waitable) {
          seen.insert(waitable.get());
          if (entities.count(waitable.get())) {return;}
          auto entity = std::make_shared<Entity>();
          entity->group = group;
          entity->guard_only = !std::dynamic_pointer_cast<
            rclcpp::experimental::SubscriptionIntraProcessBase>(waitable) &&
            waitable->get_number_of_ready_guard_conditions() > 0 &&
            waitable->get_number_of_ready_subscriptions() == 0 &&
            waitable->get_number_of_ready_timers() == 0 &&
            waitable->get_number_of_ready_clients() == 0 &&
            waitable->get_number_of_ready_services() == 0 &&
            waitable->get_number_of_ready_events() == 0;
          std::weak_ptr<Entity> weak_entity = entity;
          Waitable::WeakPtr weak_waitable = waitable;
          checked(rcl_executor_add(core, RCL_EXECUTOR_WORK, nullptr, waitable.get(),
              group_id, &entity->id));
          entity->execute = [weak_entity, weak_waitable](const rcl_executor_item_t &) {
              auto e = weak_entity.lock();
              auto work = weak_waitable.lock();
              if (!e || !work) {return false;}
              int id;
              {
                std::lock_guard<std::mutex> guard(e->mutex);
                id = e->events.front();
                e->events.pop_front();
              }
              const auto data = work->take_data_by_entity_id(static_cast<size_t>(id));
              work->execute(data);
              return true;
            };
          entity->clear = [weak_waitable] {
              if (auto work = weak_waitable.lock()) {work->clear_on_ready_callback();}
            };
          entities.emplace(waitable.get(), entity);
          waitable->set_on_ready_callback([this, weak_entity](size_t count, int id) {
              auto e = weak_entity.lock();
              if (!e) {return;}
              size_t credits = count;
              {
                std::lock_guard<std::mutex> guard(e->mutex);
                if (e->guard_only) {
                  if (std::find(e->events.begin(), e->events.end(), id) != e->events.end()) {
                    return;
                  }
                  credits = 1;
                }
                for (size_t i = 0; i < credits; ++i) {e->events.push_back(id);}
              }
              checked(rcl_executor_notify(core, e->id, credits));
            });
        };
      group->collect_all_ptrs(
        [&](const SubscriptionBase::SharedPtr & sub) {
          SubscriptionBase::WeakPtr weak_sub = sub;
          add(sub, RCL_EXECUTOR_SUBSCRIPTION, sub->get_subscription_handle(),
          [this, weak_sub](const rcl_executor_item_t &) {
            auto s = weak_sub.lock();
            if (!s) {return false;}
            owner.execute_subscription(s);
            return true;
            });
          if (auto work = sub->get_intra_process_waitable()) {add_waitable(work);}
        },
        [&](const ServiceBase::SharedPtr & service) {
          ServiceBase::WeakPtr weak_service = service;
          add(service, RCL_EXECUTOR_SERVICE, service->get_service_handle(),
          [this, weak_service](const rcl_executor_item_t &) {
            auto s = weak_service.lock();
            if (!s) {return false;}
            owner.execute_service(s);
            return true;
            });
        },
        [&](const ClientBase::SharedPtr & client) {
          ClientBase::WeakPtr weak_client = client;
          add(client, RCL_EXECUTOR_CLIENT, client->get_client_handle(),
          [this, weak_client](const rcl_executor_item_t &) {
            auto c = weak_client.lock();
            if (!c) {return false;}
            owner.execute_client(c);
            return true;
            });
        },
        [&](const TimerBase::SharedPtr & timer) {
          TimerBase::WeakPtr weak_timer = timer;
          add(timer, RCL_EXECUTOR_TIMER, timer->get_timer_handle(),
          [weak_timer](const rcl_executor_item_t & item) {
            auto t = weak_timer.lock();
            if (!t) {return false;}
            t->execute_callback(std::make_shared<rcl_timer_call_info_t>(item.timer_info));
            return true;
            });
          entities.at(timer.get())->clock = timer->get_clock();
        }, add_waitable);
    }
    for (auto it = entities.begin(); it != entities.end(); ) {
      if (seen.count(it->first)) {++it; continue;}
      if (it->second->clear) {it->second->clear();}
      checked(rcl_executor_remove(core, it->second->id));
      it = entities.erase(it);
    }
  }

  bool execute(const rcl_executor_item_t & item)
  {
    if (!rclcpp::ok(owner.context_)) {rcl_executor_stop(handle); return false;}
    if (item.registration == maintenance) {sync(); return false;}
    std::shared_ptr<Entity> entity;
    {
      std::lock_guard<std::mutex> lock(mutex);
      auto found = entities.find(item.context);
      if (found == entities.end() || found->second->id != item.registration) {return false;}
      entity = found->second;
    }
    auto group = entity->group.lock();
    if (!group) {return false;}
    bool exclusive = group->type() == CallbackGroupType::MutuallyExclusive;
    if (exclusive) {group->can_be_taken_from() = false;}
    RCPPUTILS_SCOPE_EXIT(if (exclusive) {group->can_be_taken_from() = true;});
    return entity->execute(item);
  }

  static void dispatch(const rcl_executor_item_t * item, void * data) noexcept
  {
    auto self = static_cast<Impl *>(data);
    try {
      self->execute(*item);
    } catch (...) {
      std::lock_guard<std::mutex> lock(self->mutex);
      if (!self->error) {self->error = std::current_exception();}
      rcl_executor_stop(self->handle);
    }
  }
};

RclExecutor::RclExecutor(const ExecutorOptions & options, size_t threads)
: Executor(options), impl_(std::make_unique<Impl>(*this, threads)) {}
RclExecutor::~RclExecutor() = default;
void RclExecutor::add_node(node_interfaces::NodeBaseInterface::SharedPtr node, bool notify)
{
  Executor::add_node(node, notify);
  impl_->sync();
}
void RclExecutor::remove_node(node_interfaces::NodeBaseInterface::SharedPtr node, bool notify)
{
  Executor::remove_node(node, notify);
  impl_->sync();
}
void RclExecutor::add_callback_group(
  CallbackGroup::SharedPtr group, node_interfaces::NodeBaseInterface::SharedPtr node, bool notify)
{
  Executor::add_callback_group(group, node, notify);
  impl_->sync();
}
void RclExecutor::remove_callback_group(CallbackGroup::SharedPtr group, bool notify)
{
  Executor::remove_callback_group(group, notify);
  impl_->sync();
}
void RclExecutor::cancel()
{
  spinning = false;
  rcl_executor_stop(impl_->handle);
}
void RclExecutor::spin()
{
  if (spinning.exchange(true)) {throw std::runtime_error("executor is already spinning");}
  RCPPUTILS_SCOPE_EXIT(spinning = false; rcl_executor_reset(impl_->core););
  impl_->sync();
  impl_->error = nullptr;
  auto result = rcl_executor_spin(impl_->core, impl_->threads, Impl::dispatch, impl_.get());
  if (impl_->error) {std::rethrow_exception(impl_->error);}
  checked(result);
}
void RclExecutor::spin_once_impl(std::chrono::nanoseconds timeout)
{
  RCPPUTILS_SCOPE_EXIT(rcl_executor_reset(impl_->core););
  impl_->sync();
  auto start = std::chrono::steady_clock::now();
  while (spinning && rclcpp::ok(context_)) {
    rcl_executor_item_t item;
    auto status = rcl_executor_next(impl_->core, &item);
    if (status == RCL_EXECUTOR_READY) {
      RCPPUTILS_SCOPE_EXIT(checked(rcl_executor_complete(impl_->core, item.token)););
      if (impl_->execute(item)) {return;}
      continue;
    }
    if (status == RCL_EXECUTOR_ERROR) {rclcpp::exceptions::throw_from_rcl_error(RCL_RET_ERROR);}
    if (status == RCL_EXECUTOR_STOPPED) {return;}
    auto remaining = timeout.count() < 0 ? timeout : timeout -
      std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() -
        start);
    if (timeout.count() >= 0 && remaining.count() <= 0) {return;}
    if (rcl_executor_wait(impl_->core, remaining.count()) == RCL_EXECUTOR_ERROR) {
      rclcpp::exceptions::throw_from_rcl_error(RCL_RET_ERROR);
    }
  }
}
void RclExecutor::drain(std::chrono::nanoseconds duration, bool exhaustive)
{
  if (spinning.exchange(true)) {throw std::runtime_error("executor is already spinning");}
  RCPPUTILS_SCOPE_EXIT(spinning = false; rcl_executor_reset(impl_->core););
  impl_->sync();
  auto start = std::chrono::steady_clock::now();
  std::vector<uint64_t> visited;
  while (spinning && rclcpp::ok(context_)) {
    rcl_executor_item_t item;
    auto status = exhaustive ? rcl_executor_next(impl_->core, &item) :
      rcl_executor_next_excluding(impl_->core, &item, visited.data(), visited.size());
    if (status != RCL_EXECUTOR_READY) {break;}
    RCPPUTILS_SCOPE_EXIT(checked(rcl_executor_complete(impl_->core, item.token)););
    if (!exhaustive) {visited.push_back(item.registration);}
    impl_->execute(item);
    if (duration.count() > 0 && std::chrono::steady_clock::now() - start >= duration) {break;}
  }
}
void RclExecutor::spin_some(std::chrono::nanoseconds duration) {drain(duration, false);}
void RclExecutor::spin_all(std::chrono::nanoseconds duration)
{
  if (duration.count() <= 0) {throw std::invalid_argument("spin_all duration must be positive");}
  drain(duration, true);
}
}  // namespace rclcpp::experimental::executors
