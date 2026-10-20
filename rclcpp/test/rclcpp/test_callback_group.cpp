// Copyright 2026 nev
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

#include <chrono>
#include <memory>
#include <vector>

#include "rclcpp/callback_group.hpp"
#include "rclcpp/rclcpp.hpp"

using namespace std::chrono_literals;

class TestableCallbackGroup : public rclcpp::CallbackGroup
{
public:
  using rclcpp::CallbackGroup::CallbackGroup;
  using rclcpp::CallbackGroup::add_timer;
};

class TestCallbackGroup : public ::testing::Test
{
protected:
  void SetUp() override
  {
    rclcpp::init(0, nullptr);
    node_ = std::make_shared<rclcpp::Node>("test_callback_group_node");
  }

  void TearDown() override
  {
    node_.reset();
    rclcpp::shutdown();
  }

  size_t count_timers(const rclcpp::CallbackGroup & group)
  {
    size_t count = 0;
    group.collect_all_ptrs(
      [](const rclcpp::SubscriptionBase::SharedPtr &) {},
      [](const rclcpp::ServiceBase::SharedPtr &) {},
      [](const rclcpp::ClientBase::SharedPtr &) {},
      [&count](const rclcpp::TimerBase::SharedPtr &) {++count;},
      [](const rclcpp::Waitable::SharedPtr &) {});
    return count;
  }

  rclcpp::Node::SharedPtr node_;
};

TEST_F(TestCallbackGroup, add_is_not_quadratic)
{
  TestableCallbackGroup group(
    rclcpp::CallbackGroupType::MutuallyExclusive,
    node_->get_node_base_interface()->get_context(), false);
  auto timer = node_->create_wall_timer(1h, []() {});

  // A full scan per add takes seconds here; amortized compaction takes about a millisecond.
  constexpr size_t kAdds = 20000;
  const auto start = std::chrono::steady_clock::now();
  for (size_t i = 0; i < kAdds; ++i) {
    group.add_timer(timer);
  }
  const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::steady_clock::now() - start).count();

  EXPECT_EQ(group.size(), kAdds);
  EXPECT_LT(elapsed_ms, 250);
}

TEST_F(TestCallbackGroup, expired_entries_stay_bounded)
{
  auto group = node_->create_callback_group(
    rclcpp::CallbackGroupType::MutuallyExclusive, false);
  std::vector<rclcpp::TimerBase::SharedPtr> live;
  for (size_t i = 0; i < 8; ++i) {
    live.push_back(node_->create_wall_timer(1h, []() {}, group));
  }
  for (size_t i = 0; i < 10000; ++i) {
    node_->create_wall_timer(1h, []() {}, group);
  }

  EXPECT_LE(group->size(), 64u);
  EXPECT_EQ(count_timers(*group), live.size());

  live.clear();
  EXPECT_EQ(count_timers(*group), 0u);
}
