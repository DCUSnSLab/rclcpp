// Copyright 2026 nev
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
// writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#include <rcl/wait.h>
#include <algorithm>
#include <atomic>
#include <exception>
#include <functional>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <thread>
#include <vector>

#include "rcl_executor/executor.h"
#include "rcl_executor/trace.h"
#include <rclcpp/event_handler.hpp>
#include <rclcpp/exceptions.hpp>
#include <rclcpp/logging.hpp>
#include <rcpputils/scope_exit.hpp>
#include "executor_implementation.hpp"

namespace rclcpp
{
namespace
{
void checked(rcl_ret_t result)
{
  if (result != RCL_RET_OK) {rclcpp::exceptions::throw_from_rcl_error(result);}
}

// For cleanup and notifier paths, where a throw would end the process or reach other code.
void logged(rcl_ret_t result, const char * call)
{
  if (result == RCL_RET_OK) {return;}
  RCLCPP_ERROR(rclcpp::get_logger("rclcpp"), "%s failed: %s", call, rcl_get_error_string().str);
  rcl_reset_error();
}

// The rcl event the handler adds to a wait set; the handler keeps it for its lifetime.
const rcl_event_t * event_of(Waitable & handler, rcl_context_t * context)
{
  auto ws = rcl_get_zero_initialized_wait_set();
  checked(rcl_wait_set_init(&ws, 0, 0, 0, 0, 0, 1, context, rcl_get_default_allocator()));
  RCPPUTILS_SCOPE_EXIT(logged(rcl_wait_set_fini(&ws), "rcl_wait_set_fini"););
  handler.add_to_wait_set(ws);
  return ws.events[0];
}
}  // namespace

struct ExecutorCore::Impl
{
  struct Entity
  {
    uint64_t id = 0;
    uint64_t order = UINT64_MAX;
    std::weak_ptr<const void> object;
    CallbackGroup::WeakPtr group;
    std::shared_ptr<const void> handle;
    Clock::SharedPtr clock;
    std::function<bool(const rcl_executor_item_t &)> execute;
    std::function<void()> clear;
    std::function<void(size_t, int)> on_ready;
    std::vector<uint64_t> observers;
  };
  Executor & owner;
  rcl_executor_t * core = nullptr;
  rcl_executor_handle_t * handle = nullptr;
  uint64_t maintenance = 0;
  size_t threads;
  std::mutex mutex;
  std::mutex selection_mutex;
  std::unordered_map<const void *, std::shared_ptr<Entity>> entities;
  std::exception_ptr error;
  std::atomic<bool> stopped{false};
  std::atomic<bool> dirty{true};
  bool yield_before_execute = false;
  std::chrono::nanoseconds timeout{-1};
  std::unordered_map<uint64_t, CallbackGroup::WeakPtr> known_groups;
  std::vector<uint64_t> collected_busy{UINT64_MAX};
  // Registrations of the entities that a native waitable observes.
  std::unordered_map<uint64_t, std::shared_ptr<Entity>> observed;

  Impl(Executor & executor, size_t count)
  : owner(executor), threads(std::max<size_t>(count, 1))
  {
    checked(rcl_executor_create(&core));
    checked(rcl_executor_set_trace_client(core, RCL_EXECUTOR_TRACE_RCLCPP));
    handle = rcl_executor_get_handle(core);
    checked(rcl_executor_add_group(core, UINT64_MAX, RCL_EXECUTOR_MUTUALLY_EXCLUSIVE));
    checked(rcl_executor_add(core, RCL_EXECUTOR_WORK, nullptr, nullptr, UINT64_MAX, &maintenance));
    checked(rcl_executor_set_order(core, maintenance, UINT64_MAX));
    owner.notify_waitable_->set_on_ready_callback([this](size_t, int) {
        dirty = true;
        owner.entities_need_rebuild_ = true;
        auto result = rcl_executor_notify(core, maintenance, 1);
        if (result != RCL_RET_OK) {rcl_reset_error();}
      });
  }
  ~Impl()
  {
    owner.notify_waitable_->clear_on_ready_callback();
    for (auto & pair : entities) {
      if (pair.second->clear) {pair.second->clear();}
      remove(*pair.second);
    }
    rcl_executor_destroy(core);
    rcl_executor_handle_destroy(handle);
  }

  void remove(const Entity & entity)
  {
    std::unique_lock<std::mutex> clock_guard;
    if (entity.clock) {clock_guard = std::unique_lock<std::mutex>(entity.clock->get_clock_mutex());}
    // Also called from the destructor, so failures are logged.
    logged(rcl_executor_remove(core, entity.id), "rcl_executor_remove");
    for (auto id : entity.observers) {
      logged(rcl_executor_remove(core, id), "rcl_executor_remove");
      observed.erase(id);
    }
  }

  void sync()
  {
    std::lock_guard<std::mutex> lock(mutex);
    dirty = false;
    bool changed = false;
    std::unordered_set<const void *> seen;
    std::unordered_set<uint64_t> seen_groups;
    auto groups = owner.get_all_callback_groups();
    for (auto & weak : groups) {
      auto group = weak.lock();
      if (!group) {continue;}
      auto group_id = reinterpret_cast<uintptr_t>(group.get());
      seen_groups.insert(group_id);
      known_groups[group_id] = group;
      checked(rcl_executor_add_group(core, group_id,
          group->type() == CallbackGroupType::Reentrant ?
          RCL_EXECUTOR_REENTRANT : RCL_EXECUTOR_MUTUALLY_EXCLUSIVE));
      auto add = [&](const auto & pointer, int type, auto native_handle, auto execute) {
          seen.insert(pointer.get());
          if (entities.count(pointer.get())) {return;}
          auto entity = std::make_shared<Entity>();
          entity->object = pointer;
          entity->group = group;
          entity->handle = native_handle;
          entity->execute = execute;
          checked(rcl_executor_add(core, static_cast<rcl_executor_entity_type_t>(type),
              native_handle.get(), pointer.get(), group_id, &entity->id));
          entities.emplace(pointer.get(), entity);
          changed = true;
        };
      std::function<void(const Waitable::SharedPtr &)> add_waitable;
      add_waitable = [&](const Waitable::SharedPtr & waitable) {
          seen.insert(waitable.get());
          if (entities.count(waitable.get())) {return;}
          auto entity = std::make_shared<Entity>();
          entity->object = waitable;
          entity->group = group;
          bool native_waitable = waitable->get_number_of_ready_timers() > 0;
          std::weak_ptr<Entity> weak_entity = entity;
          Waitable::WeakPtr weak_waitable = waitable;
          checked(rcl_executor_add(core, RCL_EXECUTOR_WAITABLE, nullptr, waitable.get(),
              group_id, &entity->id));
          entity->execute = [this, weak_entity, weak_waitable, native_waitable](
            const rcl_executor_item_t &) {
              auto e = weak_entity.lock();
              auto work = weak_waitable.lock();
              if (!e || !work) {return false;}
              auto ws = rcl_get_zero_initialized_wait_set();
              checked(rcl_wait_set_init(&ws,
                  work->get_number_of_ready_subscriptions(),
                  work->get_number_of_ready_guard_conditions(),
                  work->get_number_of_ready_timers(), work->get_number_of_ready_clients(),
                  work->get_number_of_ready_services(), work->get_number_of_ready_events(),
                  owner.context_->get_rcl_context().get(), rcl_get_default_allocator()));
              RCPPUTILS_SCOPE_EXIT(logged(rcl_wait_set_fini(&ws), "rcl_wait_set_fini"););
              work->add_to_wait_set(ws);
              auto result = rcl_wait(&ws, 0);
              if (result != RCL_RET_TIMEOUT) {checked(result);}
              if (!work->is_ready(ws)) {return false;}
              RCPPUTILS_SCOPE_EXIT(
                if (!native_waitable && !std::dynamic_pointer_cast<EventHandlerBase>(work)) {
              logged(rcl_executor_notify(core, e->id, 1), "rcl_executor_notify");
              }
              );
              const auto ready_data = work->take_data();
              work->execute(ready_data);
              return true;
            };
          entities.emplace(waitable.get(), entity);
          changed = true;
          if (native_waitable) {
            // The waitable owns its raw handles until its registrations are removed.
            entity->handle = waitable;
            auto ws = rcl_get_zero_initialized_wait_set();
            checked(rcl_wait_set_init(&ws,
                waitable->get_number_of_ready_subscriptions(),
                waitable->get_number_of_ready_guard_conditions(),
                waitable->get_number_of_ready_timers(), waitable->get_number_of_ready_clients(),
                waitable->get_number_of_ready_services(), waitable->get_number_of_ready_events(),
                owner.context_->get_rcl_context().get(), rcl_get_default_allocator()));
            RCPPUTILS_SCOPE_EXIT(logged(rcl_wait_set_fini(&ws), "rcl_wait_set_fini"););
            waitable->add_to_wait_set(ws);
            auto observe = [&](auto handles, size_t count, rcl_executor_entity_type_t type) {
                for (size_t i = 0; i < count; ++i) {
                  if (!handles[i]) {continue;}
                  uint64_t id;
                  checked(rcl_executor_add(core, type, handles[i], waitable.get(), group_id, &id));
                  entity->observers.push_back(id);
                  observed.emplace(id, entity);
                }
              };
            observe(ws.timers, ws.size_of_timers, RCL_EXECUTOR_TIMER_OBSERVER);
            observe(ws.subscriptions, ws.size_of_subscriptions, RCL_EXECUTOR_SUBSCRIPTION);
            observe(ws.guard_conditions, ws.size_of_guard_conditions, RCL_EXECUTOR_GUARD_CONDITION);
            observe(ws.clients, ws.size_of_clients, RCL_EXECUTOR_CLIENT);
            observe(ws.services, ws.size_of_services, RCL_EXECUTOR_SERVICE);
            observe(ws.events, ws.size_of_events, RCL_EXECUTOR_EVENT);
            return;
          }
          entity->on_ready = [this, weak_entity](size_t, int) {
              auto e = weak_entity.lock();
              if (!e) {return;}
              logged(rcl_executor_notify(core, e->id, 1), "rcl_executor_notify");
            };
          // A QoS event goes to the rcl executor callback, so a callback the user set on it stays.
          if (std::dynamic_pointer_cast<EventHandlerBase>(waitable)) {
            auto event = event_of(*waitable, owner.context_->get_rcl_context().get());
            checked(rcl_event_set_executor_callback(event,
              [](const void * data, size_t count) {
                try {
                  (*static_cast<const std::function<void(size_t, int)> *>(data))(count, 0);
                } catch (const std::exception & error) {
                  RCLCPP_ERROR(rclcpp::get_logger("rclcpp"), "executor event: %s", error.what());
                } catch (...) {
                  RCLCPP_ERROR(rclcpp::get_logger("rclcpp"), "executor event: unknown exception");
                }
              }, &entity->on_ready));
            entity->clear = [weak_waitable, event] {
                if (auto work = weak_waitable.lock()) {
                  if (rcl_event_set_executor_callback(event, nullptr, nullptr) != RCL_RET_OK) {
                    rcl_reset_error();
                  }
                }
              };
          } else {
            waitable->set_on_ready_callback(entity->on_ready);
            entity->clear = [weak_waitable] {
                if (auto work = weak_waitable.lock()) {work->clear_on_ready_callback();}
              };
          }
          checked(rcl_executor_notify(core, entity->id, 1));
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
          if (entities.count(timer.get())) {seen.insert(timer.get()); return;}
          TimerBase::WeakPtr weak_timer = timer;
          // The core edits the jump callbacks of the timer's rcl clock, which rclcpp guards.
          std::lock_guard<std::mutex> clock_guard(timer->get_clock()->get_clock_mutex());
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
      remove(*it->second);
      it = entities.erase(it);
      changed = true;
    }
    for (auto it = known_groups.begin(); it != known_groups.end(); ) {
      if (seen_groups.count(it->first)) {++it; continue;}
      checked(rcl_executor_remove_group(core, it->first));
      it = known_groups.erase(it);
    }
    reorder(changed);
  }

  void reorder(bool changed)
  {
    std::lock_guard<std::mutex> owner_lock(owner.mutex_);
    // A rebuild changes slots only if entities or the groups it leaves out changed.
    if (owner.entities_need_rebuild_.exchange(false) || owner.current_collection_.empty()) {
      auto busy = busy_groups();
      if (changed || busy != collected_busy || owner.current_collection_.empty()) {
        owner.collect_entities();
        changed = true;
        collected_busy = busy_groups() == busy ? busy : std::vector<uint64_t>{UINT64_MAX};
      }
    }
    if (!changed) {return;}
    auto order = [&](size_t count, auto get) {
        for (size_t i = 0; i < count; ++i) {
          auto pointer = get(i);
          auto found = entities.find(pointer.get());
          if (found != entities.end() && found->second->order != i) {
            checked(rcl_executor_set_order(core, found->second->id, i));
            for (auto id : found->second->observers) {
              checked(rcl_executor_set_order(core, id, i));
            }
            found->second->order = i;
          }
        }
      };
    auto & slots = owner.wait_set_;
    order(slots.size_of_timers(), [&](size_t i) {return slots.timers(i);});
    order(slots.size_of_subscriptions(), [&](size_t i) {return slots.subscriptions(i);});
    order(slots.size_of_services(), [&](size_t i) {return slots.services(i);});
    order(slots.size_of_clients(), [&](size_t i) {return slots.clients(i);});
    order(slots.size_of_waitables(), [&](size_t i) {return slots.waitables(i);});
  }

  std::vector<uint64_t> busy_groups() const
  {
    std::vector<uint64_t> busy;
    for (const auto & pair : known_groups) {
      auto group = pair.second.lock();
      if (group && !group->can_be_taken_from()) {busy.push_back(pair.first);}
    }
    std::sort(busy.begin(), busy.end());
    return busy;
  }

  // Entity destruction sends no notification; anything else that changes the collection does.
  void refresh(bool expiry)
  {
    {
      std::lock_guard<std::mutex> lock(mutex);
      bool expired = expiry && (
        std::any_of(entities.begin(), entities.end(),
        [](const auto & pair) {return pair.second->object.expired();}) ||
        std::any_of(known_groups.begin(), known_groups.end(),
        [](const auto & pair) {return pair.second.expired();}));
      if (!dirty && !expired) {
        reorder(false);
        return;
      }
    }
    sync();
  }

  bool execute(const rcl_executor_item_t & item)
  {
    if (!rclcpp::ok(owner.context_)) {rcl_executor_stop(handle); return false;}
    if (item.registration == maintenance) {sync(); return false;}
    std::shared_ptr<Entity> entity;
    {
      std::lock_guard<std::mutex> lock(mutex);
      auto found = entities.find(item.context);
      if (found != entities.end() && found->second->id == item.registration) {
        entity = found->second;
      } else {
        auto source = observed.find(item.registration);
        if (source == observed.end()) {return false;}
        entity = source->second;
      }
    }
    auto group = entity->group.lock();
    if (!group) {return false;}
    bool exclusive = group->type() == CallbackGroupType::MutuallyExclusive;
    if (exclusive) {group->can_be_taken_from() = false;}
    RCPPUTILS_SCOPE_EXIT(
      if (exclusive) {
        group->can_be_taken_from() = true;
        owner.entities_need_rebuild_ = true;
      }
    );
    return entity->execute(item);
  }

  // Executes a claimed item and releases it. A failed release is thrown here, not from a
  // destructor; while an exception from the item is already leaving, it is only logged.
  bool run_claimed(const rcl_executor_item_t & item)
  {
    bool executed;
    try {
      executed = execute(item);
    } catch (...) {
      logged(rcl_executor_complete(core, item.token), "rcl_executor_complete");
      throw;
    }
    checked(rcl_executor_complete(core, item.token));
    return executed;
  }

  void spin_wait_set()
  {
    auto run = [this]() {
        try {
          while (owner.spinning && rclcpp::ok(owner.context_)) {
            rcl_executor_item_t item;
            {
              std::unique_lock<std::mutex> selection(selection_mutex);
              auto status = rcl_executor_next(core, &item);
              if (status == RCL_EXECUTOR_TIMEOUT) {
                refresh(true);
                status = rcl_executor_wait(core, timeout.count());
                if (status == RCL_EXECUTOR_STOPPED) {return;}
                if (status == RCL_EXECUTOR_ERROR) {checked(RCL_RET_ERROR);}
                refresh(false);
                status = rcl_executor_next(core, &item);
                if (status == RCL_EXECUTOR_TIMEOUT) {
                  owner.entities_need_rebuild_ = true;
                  continue;
                }
              }
              if (status == RCL_EXECUTOR_STOPPED) {return;}
              if (status == RCL_EXECUTOR_ERROR) {checked(RCL_RET_ERROR);}
              std::lock_guard<std::mutex> lock(mutex);
              auto found = entities.find(item.context);
              if (found != entities.end()) {
                auto group = found->second->group.lock();
                if (group && group->type() == CallbackGroupType::MutuallyExclusive) {
                  group->can_be_taken_from() = false;
                }
              }
            }
            if (yield_before_execute) {std::this_thread::yield();}
            dispatch(&item, this);
            checked(rcl_executor_complete(core, item.token));
          }
        } catch (...) {
          std::lock_guard<std::mutex> lock(mutex);
          if (!error) {error = std::current_exception();}
          rcl_executor_stop(handle);
        }
      };
    std::vector<std::thread> workers;
    try {
      for (size_t i = 1; i < threads; ++i) {workers.emplace_back(run);}
    } catch (...) {
      rcl_executor_stop(handle);
      for (auto & worker : workers) {worker.join();}
      throw;
    }
    run();
    for (auto & worker : workers) {worker.join();}
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

ExecutorCore::ExecutorCore(Executor & owner, size_t threads)
: impl_(std::make_unique<Impl>(owner, threads)) {}
ExecutorCore::~ExecutorCore() = default;
void ExecutorCore::sync() {impl_->sync();}
void ExecutorCore::configure(bool yield_before_execute, std::chrono::nanoseconds timeout)
{
  impl_->yield_before_execute = yield_before_execute;
  impl_->timeout = timeout;
}
void ExecutorCore::cancel()
{
  impl_->owner.spinning = false;
  impl_->stopped = true;
  rcl_executor_stop(impl_->handle);
}
void ExecutorCore::spin()
{
  if (impl_->owner.spinning.exchange(true)) {
    throw std::runtime_error("executor is already spinning");
  }
  RCPPUTILS_SCOPE_EXIT(impl_->owner.spinning = false; rcl_executor_reset(impl_->core););
  rcl_executor_reset(impl_->core);
  impl_->owner.entities_need_rebuild_ = true;
  impl_->sync();
  impl_->error = nullptr;
  impl_->spin_wait_set();
  if (impl_->error) {std::rethrow_exception(impl_->error);}
}
void ExecutorCore::spin_once(std::chrono::nanoseconds timeout)
{
  // The round survives between calls like Executor::wait_result_; Executor ends it.
  if (impl_->stopped.exchange(false)) {rcl_executor_reset(impl_->core);}
  auto start = std::chrono::steady_clock::now();
  bool waited = false;
  while (impl_->owner.spinning && rclcpp::ok(impl_->owner.context_)) {
    rcl_executor_item_t item;
    auto status = rcl_executor_next(impl_->core, &item);
    if (status == RCL_EXECUTOR_READY) {
      if (impl_->run_claimed(item)) {return;}
      continue;
    }
    if (status == RCL_EXECUTOR_ERROR) {rclcpp::exceptions::throw_from_rcl_error(RCL_RET_ERROR);}
    if (status == RCL_EXECUTOR_STOPPED || waited) {return;}
    impl_->refresh(true);
    auto remaining = timeout;
    if (timeout.count() > 0) {
      remaining = std::max(std::chrono::nanoseconds(0), timeout -
          std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - start));
    }
    status = rcl_executor_wait(impl_->core, remaining.count());
    if (status == RCL_EXECUTOR_ERROR) {rclcpp::exceptions::throw_from_rcl_error(RCL_RET_ERROR);}
    if (status == RCL_EXECUTOR_STOPPED) {return;}
    impl_->refresh(false);
    waited = true;
  }
}
void ExecutorCore::end_round() {rcl_executor_reset(impl_->core);}
void ExecutorCore::drain(std::chrono::nanoseconds duration, bool exhaustive)
{
  if (impl_->owner.spinning.exchange(true)) {
    throw std::runtime_error("executor is already spinning");
  }
  RCPPUTILS_SCOPE_EXIT(impl_->owner.spinning = false; rcl_executor_reset(impl_->core););
  rcl_executor_reset(impl_->core);
  impl_->refresh(true);
  auto start = std::chrono::steady_clock::now();
  std::vector<uint64_t> visited;
  while (impl_->owner.spinning && rclcpp::ok(impl_->owner.context_) && duration.count() >= 0) {
    rcl_executor_item_t item;
    auto status = exhaustive ? rcl_executor_next(impl_->core, &item) :
      rcl_executor_next_excluding(impl_->core, &item, visited.data(), visited.size());
    if (status == RCL_EXECUTOR_TIMEOUT && exhaustive &&
      rcl_executor_wait(impl_->core, 0) == RCL_EXECUTOR_READY)
    {
      impl_->refresh(false);
      continue;
    }
    if (status != RCL_EXECUTOR_READY) {break;}
    if (!exhaustive) {visited.push_back(item.registration);}
    impl_->run_claimed(item);
    if (duration.count() > 0 && std::chrono::steady_clock::now() - start >= duration) {break;}
  }
}
}  // namespace rclcpp
