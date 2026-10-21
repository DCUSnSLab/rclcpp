// Copyright 2026 Open Source Robotics Foundation, Inc.
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

#include <memory>

#include "rclcpp/rclcpp.hpp"
#include "test_msgs/msg/empty.hpp"

#include "rclcpp/executors/events_cbg_executor/first_in_first_out_scheduler.hpp"

using rclcpp::executors::cbg_executor::FirstInFirstOutScheduler;

class TestEventsCBGExecutorQueue : public ::testing::Test
{
public:
  void SetUp()
  {
    rclcpp::init(0, nullptr);
    node = std::make_shared<rclcpp::Node>("queue_node");
    group = node->create_callback_group(rclcpp::CallbackGroupType::MutuallyExclusive, false);
  }

  void TearDown()
  {
    group.reset();
    node.reset();
    rclcpp::shutdown();
  }

  rclcpp::SubscriptionBase::SharedPtr create_subscription(const rclcpp::QoS & qos)
  {
    rclcpp::SubscriptionOptions options;
    options.callback_group = group;
    return node->create_subscription<test_msgs::msg::Empty>(
      "queue_topic", qos, [](test_msgs::msg::Empty::ConstSharedPtr) {}, options);
  }

  size_t drain(FirstInFirstOutScheduler & scheduler)
  {
    size_t executed = 0;
    while (auto ready = scheduler.get_next_ready_entity().entity) {
      scheduler.mark_entity_as_executed(*ready);
      ++executed;
    }
    return executed;
  }

  rclcpp::Node::SharedPtr node;
  rclcpp::CallbackGroup::SharedPtr group;
};

// ros2/rclcpp#3213: a busy KEEP_LAST subscription keeps at most depth events queued.
TEST_F(TestEventsCBGExecutorQueue, keep_last_subscription_events_bounded_by_depth)
{
  auto subscription = create_subscription(rclcpp::QoS(5));
  FirstInFirstOutScheduler scheduler([]() {});
  auto ready = scheduler.add_callback_group(group)->get_ready_callback_for_entity(
    rclcpp::SubscriptionBase::WeakPtr(subscription));

  ready(100);
  ready(1);
  EXPECT_EQ(5u, drain(scheduler));

  ready(3);
  EXPECT_EQ(3u, drain(scheduler));

  ready(2);
  auto executing = scheduler.get_next_ready_entity().entity;
  ASSERT_TRUE(executing);
  ready(10);
  scheduler.mark_entity_as_executed(*executing);
  EXPECT_EQ(5u, drain(scheduler));
}

TEST_F(TestEventsCBGExecutorQueue, keep_all_subscription_events_not_bounded)
{
  auto subscription = create_subscription(rclcpp::QoS(rclcpp::KeepAll()));
  FirstInFirstOutScheduler scheduler([]() {});
  auto ready = scheduler.add_callback_group(group)->get_ready_callback_for_entity(
    rclcpp::SubscriptionBase::WeakPtr(subscription));

  ready(100);
  EXPECT_EQ(100u, drain(scheduler));
}
