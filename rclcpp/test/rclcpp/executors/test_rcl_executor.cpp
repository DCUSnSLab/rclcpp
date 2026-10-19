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
