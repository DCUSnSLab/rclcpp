// Copyright 2018 Open Source Robotics Foundation, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <sys/resource.h>

#include <atomic>
#include <chrono>
#include <future>
#include <string>
#include <memory>
#include <thread>

#include "rclcpp/exceptions.hpp"
#include "rclcpp/node.hpp"
#include "rclcpp/rclcpp.hpp"
#include "rclcpp/executors.hpp"
#include "test_msgs/msg/empty.hpp"

using namespace std::chrono_literals;

class TestMultiThreadedExecutor : public ::testing::Test
{
protected:
  static void SetUpTestCase()
  {
    rclcpp::init(0, nullptr);
  }
};

constexpr std::chrono::milliseconds PERIOD_MS = 1000ms;
constexpr double PERIOD = PERIOD_MS.count() / 1000.0;
constexpr double TOLERANCE = PERIOD / 4.0;

/*
   Test that timers are not taken multiple times when using reentrant callback groups.
 */
TEST_F(TestMultiThreadedExecutor, timer_over_take) {
#ifdef __linux__
  // This seems to be the most effective way to force the bug to happen on Linux.
  // This is unnecessary on MacOS, since the default scheduler causes it.
  struct sched_param param;
  param.sched_priority = 0;
  if (sched_setscheduler(0, SCHED_BATCH, &param) != 0) {
    perror("sched_setscheduler");
  }
#endif

  bool yield_before_execute = true;

  rclcpp::executors::MultiThreadedExecutor executor(
    rclcpp::ExecutorOptions(), 2u, yield_before_execute);

  ASSERT_GT(executor.get_number_of_threads(), 1u);

  std::shared_ptr<rclcpp::Node> node =
    std::make_shared<rclcpp::Node>("test_multi_threaded_executor_timer_over_take");

  auto cbg = node->create_callback_group(rclcpp::CallbackGroupType::Reentrant);

  rclcpp::Clock system_clock(RCL_STEADY_TIME);
  std::mutex last_mutex;
  auto last = system_clock.now();

  std::atomic_int timer_count {0};

  auto timer_callback = [&timer_count, &executor, &system_clock, &last_mutex, &last]() {
      // While this tolerance is a little wide, if the bug occurs, the next step will
      // happen almost instantly. The purpose of this test is not to measure the jitter
      // in timers, just assert that a reasonable amount of time has passed.
      rclcpp::Time now = system_clock.now();
      timer_count++;

      if (timer_count > 5) {
        executor.cancel();
      }

      {
        std::lock_guard<std::mutex> lock(last_mutex);
        double diff = static_cast<double>(std::abs((now - last).nanoseconds())) / 1.0e9;
        last = now;

        if (diff < PERIOD - TOLERANCE) {
          executor.cancel();
          ASSERT_GT(diff, PERIOD - TOLERANCE);
        }
      }
    };

  auto timer = node->create_wall_timer(PERIOD_MS, timer_callback, cbg);
  executor.add_node(node);
  executor.spin();
}

/*
   Test that the threads left waiting do not spin while a mutually exclusive subscription
   callback runs and more messages for it are pending.
 */
TEST_F(TestMultiThreadedExecutor, no_busy_wait_while_exclusive_group_runs) {
#ifdef __linux__
  auto cpu_s = []() {
      rusage u;
      getrusage(RUSAGE_SELF, &u);
      return u.ru_utime.tv_sec + u.ru_stime.tv_sec +
             (u.ru_utime.tv_usec + u.ru_stime.tv_usec) / 1e6;
    };
  rclcpp::executors::MultiThreadedExecutor executor(rclcpp::ExecutorOptions(), 2u);
  auto node = std::make_shared<rclcpp::Node>("no_busy_wait");
  std::atomic<int> count{0};
  std::promise<void> second_started;
  auto sub = node->create_subscription<test_msgs::msg::Empty>(
    "no_busy_wait", rclcpp::QoS(10).reliable(), [&](test_msgs::msg::Empty::ConstSharedPtr) {
      if (++count == 2) {
        second_started.set_value();
      }
      std::this_thread::sleep_for(400ms);
    });
  auto pub = node->create_publisher<test_msgs::msg::Empty>(
    "no_busy_wait", rclcpp::QoS(10).reliable());
  executor.add_node(node);
  std::thread spinner([&executor]() {executor.spin();});
  auto end = std::chrono::steady_clock::now() + 5s;
  while (pub->get_subscription_count() < 1 && std::chrono::steady_clock::now() < end) {
    std::this_thread::sleep_for(10ms);
  }
  for (int i = 0; i < 3; ++i) {
    pub->publish(test_msgs::msg::Empty());
  }
  // Each later callback starts as the group is released: measure while the second one runs
  // and the third message waits.
  ASSERT_EQ(std::future_status::ready, second_started.get_future().wait_for(5s));
  std::this_thread::sleep_for(50ms);
  double cpu = cpu_s();
  std::this_thread::sleep_for(300ms);
  cpu = cpu_s() - cpu;
  end = std::chrono::steady_clock::now() + 5s;
  while (count < 3 && std::chrono::steady_clock::now() < end) {
    std::this_thread::sleep_for(10ms);
  }
  executor.cancel();
  spinner.join();
  EXPECT_EQ(3, count.load());
  // A thread spinning on the pending message takes a whole core (0.3 s here).
  EXPECT_LT(cpu, 0.09) << "cpu seconds used while the callback slept";
#endif
}
