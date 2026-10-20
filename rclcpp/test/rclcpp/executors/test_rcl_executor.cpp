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
#include <thread>
#include <cstdlib>
#include <string>
#include <vector>

#include <test_msgs/msg/basic_types.hpp>
#include <test_msgs/srv/basic_types.hpp>
#include "test_waitable.hpp"

#include <rclcpp/experimental/executors/rcl_executor.hpp>
#include <rclcpp/rclcpp.hpp>

TEST(RclExecutor, spin_some_does_not_claim_a_second_timer_tick)
{
  rclcpp::init(0, nullptr);
  auto node = std::make_shared<rclcpp::Node>("spin_some_timer");
  rclcpp::experimental::executors::RclExecutor executor;
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

TEST(RclExecutor, future_spin_restarts_after_cancel)
{
  rclcpp::init(0, nullptr);
  auto node = std::make_shared<rclcpp::Node>("executor_restart");
  rclcpp::experimental::executors::RclExecutor executor;
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
using RclExecutor = rclcpp::experimental::executors::RclExecutor;

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
  rclcpp::Executor * active = nullptr;

  void record(const std::string & name)
  {
    sequence.push_back(name);
    if (sequence.size() == (arrivals ? 14u : 12u)) {active->cancel();}
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
    std::promise<void> done;
    auto future = done.get_future();
    std::thread watchdog([&] {
        if (future.wait_for(2s) == std::future_status::timeout) {executor.cancel();}
      });
    executor.spin();
    done.set_value();
    watchdog.join();
    EXPECT_EQ(expected, sequence.size());
    executor.remove_node(node);
    peer_executor.spin_some();
    peer_client->remove_pending_request(response);
    client->remove_pending_request(request);
    return sequence;
  }
};

void compare_policy(bool arrivals)
{
  rclcpp::init(0, nullptr);
  {
    PolicyScenario scenario(true, arrivals);
    rclcpp::executors::SingleThreadedExecutor reference;
    auto policy = std::getenv("RCL_EXECUTOR_EQUIVALENCE_FIFO") ?
      RCL_EXECUTOR_FIFO : RCL_EXECUTOR_WAIT_SET;
    for (int repeat = 0; repeat < 20; ++repeat) {
      SCOPED_TRACE(repeat);
      auto expected = scenario.run(reference);
      RclExecutor candidate(rclcpp::ExecutorOptions(), 1, policy);
      EXPECT_EQ(expected, scenario.run(candidate));
    }
  }
  rclcpp::shutdown();
}
}  // namespace

TEST(RclExecutorPolicy, single_threaded_backlogs)
{
  compare_policy(false);
}

TEST(RclExecutorPolicy, single_threaded_arrivals_during_callback)
{
  compare_policy(true);
}

namespace
{
void compare_threaded_policy(bool arrivals)
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
    auto policy = std::getenv("RCL_EXECUTOR_EQUIVALENCE_FIFO") ?
      RCL_EXECUTOR_FIFO : RCL_EXECUTOR_WAIT_SET;
    for (int repeat = 0; repeat < 20; ++repeat) {
      SCOPED_TRACE(repeat);
      rclcpp::executors::MultiThreadedExecutor reference(rclcpp::ExecutorOptions(), 3);
      auto expected = run(reference);
      RclExecutor candidate(rclcpp::ExecutorOptions(), 3, policy);
      EXPECT_EQ(expected, run(candidate));
    }
  }
  rclcpp::shutdown();
}
}  // namespace

TEST(RclExecutorPolicy, multi_threaded_exclusive_backlog)
{
  compare_threaded_policy(false);
}

TEST(RclExecutorPolicy, multi_threaded_exclusive_arrivals)
{
  compare_threaded_policy(true);
}

TEST(RclExecutorPolicy, priority_drains_high_group_before_low_group)
{
  rclcpp::init(0, nullptr);
  {
    auto node = std::make_shared<rclcpp::Node>("priority_groups");
    auto high = node->create_callback_group(rclcpp::CallbackGroupType::MutuallyExclusive);
    rclcpp::SubscriptionOptions options;
    options.callback_group = high;
    std::vector<std::string> sequence;
    auto low_sub = node->create_subscription<Message>("priority", 10,
        [&](Message::ConstSharedPtr) {sequence.push_back("low");});
    auto high_sub = node->create_subscription<Message>("priority", 10,
        [&](Message::ConstSharedPtr) {sequence.push_back("high");}, options);
    auto pub = node->create_publisher<Message>("priority", 10);
    RclExecutor executor(rclcpp::ExecutorOptions(), 1, RCL_EXECUTOR_PRIORITY);
    executor.add_node(node);
    executor.set_group_priority(node->get_node_base_interface()->get_default_callback_group(), -1);
    executor.set_group_priority(high, 5);
    pub->publish(Message());
    pub->publish(Message());
    std::this_thread::sleep_for(20ms);
    auto deadline = std::chrono::steady_clock::now() + 1s;
    while (sequence.size() < 4 && std::chrono::steady_clock::now() < deadline) {
      executor.spin_once(100ms);
    }
    EXPECT_EQ((std::vector<std::string>{"high", "high", "low", "low"}), sequence);
    executor.remove_node(node);
  }
  rclcpp::shutdown();
}
