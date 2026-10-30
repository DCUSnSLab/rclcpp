// Copyright 2026 nev
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
// writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#include <gtest/gtest.h>
#include <chrono>
#include <future>
#include <array>
#include <atomic>
#include <iostream>
#include <map>
#include <mutex>
#include <thread>
#include <cstdlib>
#include <string>
#include <vector>

#include <test_msgs/msg/basic_types.hpp>
#include <test_msgs/srv/basic_types.hpp>
#include "test_waitable.hpp"

#include <rclcpp/rclcpp.hpp>

namespace
{
// Restores ROS_EXECUTOR_CORE when the test ends.
class CoreSwitch
{
public:
  CoreSwitch()
  : had_(std::getenv("ROS_EXECUTOR_CORE") != nullptr),
    saved_(had_ ? std::getenv("ROS_EXECUTOR_CORE") : "") {}
  ~CoreSwitch()
  {
    if (had_) {set(saved_.c_str());} else {set(nullptr);}
  }
  void set(const char * value)
  {
    if (value) {setenv("ROS_EXECUTOR_CORE", value, 1);} else {unsetenv("ROS_EXECUTOR_CORE");}
  }

private:
  bool had_;
  std::string saved_;
};

// A Single/MultiThreadedExecutor that runs on the rcl_executor core.
std::unique_ptr<rclcpp::Executor> make_core_executor(size_t threads = 1)
{
  CoreSwitch core;
  core.set("1");
  if (threads == 1) {return std::make_unique<rclcpp::executors::SingleThreadedExecutor>();}
  return std::make_unique<rclcpp::executors::MultiThreadedExecutor>(
    rclcpp::ExecutorOptions(), threads);
}

class LegacyTimerWaitable : public rclcpp::Waitable
{
public:
  LegacyTimerWaitable(rclcpp::Node & node, std::function<void()> callback)
  : clock_(node.get_clock()), context_(node.get_node_base_interface()->get_context()),
    callback_(std::move(callback))
  {
    auto ret = rcl_timer_init2(&timer_, clock_->get_clock_handle(),
        context_->get_rcl_context().get(), 5000000, nullptr, rcl_get_default_allocator(), true);
    if (ret != RCL_RET_OK) {rclcpp::exceptions::throw_from_rcl_error(ret);}
  }
  ~LegacyTimerWaitable() override {EXPECT_EQ(RCL_RET_OK, rcl_timer_fini(&timer_));}
  size_t get_number_of_ready_timers() override {return 1;}
  void add_to_wait_set(rcl_wait_set_t & wait) override
  {
    ASSERT_EQ(RCL_RET_OK, rcl_wait_set_add_timer(&wait, &timer_, &index_));
  }
  bool is_ready(const rcl_wait_set_t & wait) override {return wait.timers[index_] != nullptr;}
  std::shared_ptr<void> take_data() override {return nullptr;}
  void execute(const std::shared_ptr<void> &) override
  {
    ASSERT_EQ(RCL_RET_OK, rcl_timer_call(&timer_));
    callback_();
  }

private:
  rcl_timer_t timer_ = rcl_get_zero_initialized_timer();
  size_t index_ = 0;
  rclcpp::Clock::SharedPtr clock_;
  rclcpp::Context::SharedPtr context_;
  std::function<void()> callback_;
};
}  // namespace

TEST(ExecutorCore, legacy_waitable_timer_wakes_repeats_and_detaches)
{
  rclcpp::init(0, nullptr);
  for (size_t threads : {1u, 2u}) {
    auto node = std::make_shared<rclcpp::Node>("legacy_waitable_timer");
    auto group = node->create_callback_group(rclcpp::CallbackGroupType::Reentrant);
    auto core = make_core_executor(threads);
    auto & executor = *core;
    int calls = 0;
    auto work = std::make_shared<LegacyTimerWaitable>(*node, [&] {
          if (++calls == 3) {executor.cancel();}
        });
    node->get_node_waitables_interface()->add_waitable(work, group);
    auto timeout = node->create_wall_timer(std::chrono::seconds(1), [&] {executor.cancel();});
    executor.add_node(node);
    executor.spin();
    EXPECT_EQ(3, calls);
    node->get_node_waitables_interface()->remove_waitable(work, group);
    node->get_node_base_interface()->trigger_notify_guard_condition();
    executor.spin_some();
    std::weak_ptr<LegacyTimerWaitable> released = work;
    work.reset();
    EXPECT_TRUE(released.expired());
    executor.remove_node(node);
  }
  rclcpp::shutdown();
}

TEST(ExecutorCore, spin_some_does_not_claim_a_second_timer_tick)
{
  rclcpp::init(0, nullptr);
  auto node = std::make_shared<rclcpp::Node>("spin_some_timer");
  auto core = make_core_executor();
  auto & executor = *core;
  int calls = 0;
  auto timer = node->create_wall_timer(std::chrono::milliseconds(1), [&] {
        ++calls;
        std::this_thread::sleep_for(std::chrono::milliseconds(4));
    });
  timer->cancel();
  executor.add_node(node);
  for (int i = 0; i < 16; ++i) {
    executor.spin_some();
                                                     }
  timer->reset();
  std::this_thread::sleep_for(std::chrono::milliseconds(2));
  executor.spin_some();
  EXPECT_EQ(1, calls);
  EXPECT_TRUE(timer->is_ready());
  executor.remove_node(node);
  rclcpp::shutdown();
}

TEST(ExecutorCore, future_spin_restarts_after_cancel)
{
  rclcpp::init(0, nullptr);
  auto node = std::make_shared<rclcpp::Node>("executor_restart");
  auto core = make_core_executor();
  auto & executor = *core;
  executor.add_node(node);
  std::promise<int> first;
  auto first_future = first.get_future().share();
  auto cancel = node->create_wall_timer(std::chrono::milliseconds(2), [&] {executor.cancel();});
  EXPECT_EQ(rclcpp::FutureReturnCode::INTERRUPTED,
    executor.spin_until_future_complete(first_future, std::chrono::milliseconds(100)));
  cancel->cancel();
  std::promise<int> second;
  auto second_future = second.get_future().share();
  auto finish = node->create_wall_timer(std::chrono::milliseconds(2), [&] {second.set_value(66);});
  EXPECT_EQ(rclcpp::FutureReturnCode::SUCCESS,
    executor.spin_until_future_complete(second_future, std::chrono::milliseconds(100)));
  executor.remove_node(node);
  rclcpp::shutdown();
}

namespace
{
using namespace std::chrono_literals;
using Message = test_msgs::msg::BasicTypes;
using Service = test_msgs::srv::BasicTypes;

class PolicyScenario
{
public:
  rclcpp::Node::SharedPtr node = std::make_shared<rclcpp::Node>("policy_scenario");
  rclcpp::Node::SharedPtr peer = std::make_shared<rclcpp::Node>("policy_peer");
  rclcpp::executors::SingleThreadedExecutor peer_executor;
  std::vector<std::string> sequence;
  rclcpp::CallbackGroup::SharedPtr other;
  std::vector<rclcpp::Subscription<Message>::SharedPtr> subscriptions;
  std::vector<rclcpp::Publisher<Message>::SharedPtr> publishers;
  std::vector<rclcpp::TimerBase::SharedPtr> timers;
  rclcpp::TimerBase::SharedPtr late;
  std::shared_ptr<TestWaitable> waitable = std::make_shared<TestWaitable>();
  rclcpp::Service<Service>::SharedPtr service, peer_service;
  rclcpp::Client<Service>::SharedPtr client, peer_client;
  int requests = 0;
  bool arrivals;
  enum class Drive {Spin, SpinOnce, Future} drive = Drive::Spin;
  std::promise<void> finished;
  rclcpp::Executor * active = nullptr;

  void record(const std::string & name)
  {
    sequence.push_back(name);
    if (sequence.size() == (arrivals ? 14u : 12u)) {
      if (drive == Drive::Future) {finished.set_value();} else {active->cancel();}
    }
  }

  PolicyScenario(bool two_groups, bool inject)
  : arrivals(inject)
  {
    other = two_groups ? node->create_callback_group(rclcpp::CallbackGroupType::Reentrant) :
      node->get_node_base_interface()->get_default_callback_group();
    for (int i = 0; i < 3; ++i) {
      rclcpp::SubscriptionOptions options;
      options.callback_group = i == 1 ? other : nullptr;
      auto topic = "policy_topic_" + std::to_string(i);
      subscriptions.push_back(node->create_subscription<Message>(topic, 100,
        [this, i](Message::ConstSharedPtr msg) {
          record("sub" + std::to_string(i) + ":" + std::to_string(msg->int32_value));
          if (arrivals && i == 0 && msg->int32_value == 0) {
            late->reset();
            Message extra;
            extra.int32_value = 9;
            publishers[2]->publish(extra);
            std::this_thread::sleep_for(10ms);
          }
          }, options));
      publishers.push_back(node->create_publisher<Message>(topic, 100));
    }
    for (int i = 0; i < 3; ++i) {
      timers.push_back(node->create_wall_timer(0ns, [this, i] {
          timers[i]->cancel();
          record("timer" + std::to_string(i));
          }, i == 1 ? other : nullptr));
      timers.back()->cancel();
    }
    late = node->create_wall_timer(5ms, [this] {
          late->cancel();
          record("late_timer");
        });
    late->cancel();
    waitable->set_on_execute_callback([this] {record("waitable");});
    node->get_node_waitables_interface()->add_waitable(waitable, other);
    service = node->create_service<Service>("policy_service", [this](
          const std::shared_ptr<Service::Request>, std::shared_ptr<Service::Response>) {
          record("service");
      });
    peer_service = peer->create_service<Service>("policy_peer_service", [this](
          const std::shared_ptr<Service::Request>, std::shared_ptr<Service::Response>) {
          ++requests;
      });
    client = node->create_client<Service>("policy_peer_service");
    peer_client = peer->create_client<Service>("policy_service");
    peer_executor.add_node(peer);
    EXPECT_TRUE(client->wait_for_service(2s));
    EXPECT_TRUE(peer_client->wait_for_service(2s));
  }

  std::vector<std::string> run(rclcpp::Executor & executor)
  {
    sequence.clear();
    active = &executor;
    executor.add_node(node);
    // Establish every initial ready entity before either executor takes its snapshot.
    for (int value = 0; value < 2; ++value) {
      for (auto & pub : publishers) {
        Message msg;
        msg.int32_value = value;
        pub->publish(msg);
      }
    }
    auto previous = requests;
    auto response = peer_client->async_send_request(std::make_shared<Service::Request>());
    auto request = client->async_send_request(std::make_shared<Service::Request>(),
        [this](rclcpp::Client<Service>::SharedFuture) {record("client");});
    auto deadline = std::chrono::steady_clock::now() + 2s;
    while (requests == previous && std::chrono::steady_clock::now() < deadline) {
      peer_executor.spin_once(10ms);
    }
    EXPECT_EQ(previous + 1, requests);
    std::this_thread::sleep_for(20ms);
    for (auto & timer : timers) {timer->reset();}
    waitable->trigger();
    const size_t expected = arrivals ? 14 : 12;
    if (drive == Drive::Future) {
      finished = std::promise<void>();
      executor.spin_until_future_complete(finished.get_future(), 2s);
    } else if (drive == Drive::SpinOnce) {
      auto until = std::chrono::steady_clock::now() + 2s;
      while (sequence.size() < expected && std::chrono::steady_clock::now() < until) {
        executor.spin_once(50ms);
      }
    } else {
      std::promise<void> done;
      auto future = done.get_future();
      std::thread watchdog([&] {
          if (future.wait_for(2s) == std::future_status::timeout) {executor.cancel();}
        });
      executor.spin();
      done.set_value();
      watchdog.join();
    }
    EXPECT_EQ(expected, sequence.size());
    executor.remove_node(node);
    peer_executor.spin_some();
    peer_client->remove_pending_request(response);
    client->remove_pending_request(request);
    return sequence;
  }
};

void compare_default_executor(bool arrivals, PolicyScenario::Drive drive)
{
  CoreSwitch core;
  core.set(nullptr);
  rclcpp::init(0, nullptr);
  {
    PolicyScenario scenario(true, arrivals);
    scenario.drive = drive;
    rclcpp::executors::SingleThreadedExecutor reference;
    core.set("1");
    rclcpp::executors::SingleThreadedExecutor candidate;
    core.set(nullptr);
    for (int repeat = 0; repeat < 20; ++repeat) {
      SCOPED_TRACE(repeat);
      EXPECT_EQ(scenario.run(reference), scenario.run(candidate));
    }
  }
  rclcpp::shutdown();
}
}  // namespace

TEST(ExecutorCoreEquivalence, single_threaded_backlogs)
{
  compare_default_executor(false, PolicyScenario::Drive::Spin);
}

TEST(ExecutorCoreEquivalence, single_threaded_arrivals_during_callback)
{
  compare_default_executor(true, PolicyScenario::Drive::Spin);
}

TEST(ExecutorCoreEquivalence, single_threaded_spin_once_backlogs)
{
  compare_default_executor(false, PolicyScenario::Drive::SpinOnce);
}

TEST(ExecutorCoreEquivalence, single_threaded_future_backlogs)
{
  compare_default_executor(false, PolicyScenario::Drive::Future);
}

TEST(ExecutorCoreEquivalence, single_threaded_spin_once_arrivals_during_callback)
{
  compare_default_executor(true, PolicyScenario::Drive::SpinOnce);
}

TEST(ExecutorCoreEquivalence, single_threaded_future_arrivals_during_callback)
{
  compare_default_executor(true, PolicyScenario::Drive::Future);
}

// rclcpp::spin_some(node) builds a new executor per call; samples announced to the previous
// executor's readiness callback must still reach the next one.
TEST(ExecutorCoreEquivalence, temporary_executors_keep_unconsumed_messages)
{
  CoreSwitch core;
  core.set("1");
  rclcpp::init(0, nullptr);
  {
    auto node = std::make_shared<rclcpp::Node>("temporary_executors");
    int received = 0;
    auto sub = node->create_subscription<Message>("temporary_executors", 10,
        [&](Message::ConstSharedPtr) {++received;});
    auto pub = node->create_publisher<Message>("temporary_executors", 10);
    rclcpp::spin_some(node);
    for (int i = 0; i < 3; ++i) {
      pub->publish(Message());
    }
    auto deadline = std::chrono::steady_clock::now() + 2s;
    while (sub->get_publisher_count() == 0 && std::chrono::steady_clock::now() < deadline) {}
    std::this_thread::sleep_for(100ms);
    for (int i = 0; i < 3; ++i) {
      rclcpp::spin_some(node);
    }
    EXPECT_EQ(3, received);
  }
  rclcpp::shutdown();
}

namespace
{
void compare_threaded_executor(bool arrivals)
{
  rclcpp::init(0, nullptr);
  {
    auto node = std::make_shared<rclcpp::Node>("policy_threaded");
    std::vector<std::string> sequence;
    std::vector<rclcpp::Publisher<Message>::SharedPtr> publishers;
    std::vector<rclcpp::Subscription<Message>::SharedPtr> subscriptions;
    rclcpp::Executor * active = nullptr;
    for (int i = 0; i < 3; ++i) {
      auto topic = "threaded_policy_" + std::to_string(i);
      publishers.push_back(node->create_publisher<Message>(topic, 100));
      subscriptions.push_back(node->create_subscription<Message>(topic, 100,
        [&, i](Message::ConstSharedPtr msg) {
          sequence.push_back(std::to_string(i) + ":" + std::to_string(msg->int32_value));
          if (sequence.size() == 12) {active->cancel(); return;}
          if (arrivals) {
            Message next;
            next.int32_value = msg->int32_value + 1;
            publishers[(i + 1) % 3]->publish(next);
            std::this_thread::sleep_for(1ms);
          }
          }));
    }
    auto run = [&](rclcpp::Executor & executor) {
        sequence.clear();
        active = &executor;
        executor.add_node(node);
        for (int i = 0; i < (arrivals ? 1 : 12); ++i) {
          Message msg;
          msg.int32_value = i;
          publishers[0]->publish(msg);
        }
        std::this_thread::sleep_for(10ms);
        std::promise<void> done;
        auto future = done.get_future();
        std::thread watchdog([&] {
            if (future.wait_for(2s) == std::future_status::timeout) {executor.cancel();}
          });
        executor.spin();
        done.set_value();
        watchdog.join();
        EXPECT_EQ(12u, sequence.size());
        executor.remove_node(node);
        return sequence;
      };
    for (int repeat = 0; repeat < 20; ++repeat) {
      SCOPED_TRACE(repeat);
      rclcpp::executors::MultiThreadedExecutor reference(rclcpp::ExecutorOptions(), 3);
      auto expected = run(reference);
      auto candidate = make_core_executor(3);
      EXPECT_EQ(expected, run(*candidate));
    }
  }
  rclcpp::shutdown();
}
}  // namespace

TEST(ExecutorCoreEquivalence, multi_threaded_exclusive_backlog)
{
  compare_threaded_executor(false);
}

TEST(ExecutorCoreEquivalence, multi_threaded_exclusive_arrivals)
{
  compare_threaded_executor(true);
}

namespace
{
class TwoGuards : public rclcpp::Waitable
{
public:
  std::array<rclcpp::GuardCondition, 2> guards;
  std::array<bool, 2> pending{};
  size_t selected = 0;
  rclcpp::Executor * executor = nullptr;
  std::vector<size_t> sequence;
  size_t get_number_of_ready_guard_conditions() override {return 2;}
  void add_to_wait_set(rcl_wait_set_t & waitset) override
  {
    for (size_t i = 0; i < 2; ++i) {
      if (pending[i]) {guards[i].trigger();}
      auto result = rcl_wait_set_add_guard_condition(&waitset, &guards[i].get_rcl_guard_condition(),
          nullptr);
      if (result != RCL_RET_OK) {throw std::runtime_error("add guard");}
    }
  }
  bool is_ready(const rcl_wait_set_t & waitset) override
  {
    for (size_t i = 0; i < 2; ++i) {
      for (size_t j = 0; j < waitset.size_of_guard_conditions; ++j) {
        if (pending[i] && waitset.guard_conditions[j] == &guards[i].get_rcl_guard_condition()) {
          selected = i;
          return true;
        }
      }
    }
    return false;
  }
  std::shared_ptr<void> take_data() override {return std::make_shared<size_t>(selected);}
  std::shared_ptr<void> take_data_by_entity_id(size_t id) override
  {
    return std::make_shared<size_t>(id);
  }
  void execute(const std::shared_ptr<void> & data) override
  {
    auto id = *std::static_pointer_cast<size_t>(data);
    pending[id] = false;
    sequence.push_back(id);
    if (sequence.size() == 2) {executor->cancel();}
  }
  void set_on_ready_callback(std::function<void(size_t, int)> callback) override
  {
    for (int i = 0; i < 2; ++i) {
      guards[i].set_on_trigger_callback([callback, i](size_t count) {callback(count, i);});
    }
  }
  void clear_on_ready_callback() override
  {
    for (auto & guard : guards) {guard.set_on_trigger_callback(nullptr);}
  }
};
}  // namespace

TEST(ExecutorCore, composite_waitable_chooses_the_ready_entity)
{
  rclcpp::init(0, nullptr);
  {
    auto node = std::make_shared<rclcpp::Node>("composite_waitable_order");
    auto work = std::make_shared<TwoGuards>();
    auto group = node->get_node_base_interface()->get_default_callback_group();
    node->get_node_waitables_interface()->add_waitable(work, group);
    auto core = make_core_executor();
    auto & executor = *core;
    work->executor = &executor;
    executor.add_node(node);
    for (int i : {1, 0}) {
      work->pending[i] = true; work->guards[i].trigger();
    }
    std::promise<void> done;
    auto future = done.get_future();
    std::thread watchdog([&] {
        if (future.wait_for(2s) == std::future_status::timeout) {executor.cancel();}
      });
    executor.spin();
    done.set_value();
    watchdog.join();
    EXPECT_EQ((std::vector<size_t>{0, 1}), work->sequence);
    executor.remove_node(node);
  }
  rclcpp::shutdown();
}
