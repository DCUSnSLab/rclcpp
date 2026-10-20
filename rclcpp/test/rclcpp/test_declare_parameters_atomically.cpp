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

#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "rcl_interfaces/msg/parameter_event.hpp"
#include "rclcpp/rclcpp.hpp"

using namespace std::chrono_literals;
using rcl_interfaces::msg::ParameterDescriptor;
using rcl_interfaces::msg::ParameterEvent;

class TestDeclareParametersAtomically : public ::testing::Test
{
protected:
  static void SetUpTestCase() {rclcpp::init(0, nullptr);}

  static void TearDownTestCase() {rclcpp::shutdown();}

  void SetUp() override
  {
    const auto * info = ::testing::UnitTest::GetInstance()->current_test_info();
    name_ = info->name();
    listener_ = std::make_shared<rclcpp::Node>(name_ + "_listener");
    events_ = listener_->create_subscription<ParameterEvent>(
      "/parameter_events", rclcpp::ParameterEventsQoS(),
      [this](ParameterEvent::ConstSharedPtr event) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (event->node == "/" + name_) {
          received_.push_back(*event);
        }
      });
    executor_.add_node(listener_);
    spinner_ = std::thread([this]() {executor_.spin();});
  }

  void TearDown() override
  {
    executor_.cancel();
    spinner_.join();
  }

  rclcpp::Node::SharedPtr make_node(rclcpp::NodeOptions options = rclcpp::NodeOptions())
  {
    auto node = std::make_shared<rclcpp::Node>(name_, options);
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (node->count_subscribers("/parameter_events") == 0 &&
      std::chrono::steady_clock::now() < deadline)
    {
      std::this_thread::sleep_for(10ms);
    }
    // Drop the events of the declarations done by the node constructor.
    settle();
    std::lock_guard<std::mutex> lock(mutex_);
    received_.clear();
    return node;
  }

  // Waits until no further event arrives for a while.
  std::vector<ParameterEvent> settle()
  {
    size_t count = 0;
    do {
      {
        std::lock_guard<std::mutex> lock(mutex_);
        count = received_.size();
      }
      std::this_thread::sleep_for(500ms);
    } while (count != events_count());
    std::lock_guard<std::mutex> lock(mutex_);
    return received_;
  }

  size_t events_count()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return received_.size();
  }

  static std::vector<rclcpp::Parameter> numbered(size_t count)
  {
    std::vector<rclcpp::Parameter> parameters;
    for (size_t i = 0; i < count; ++i) {
      parameters.emplace_back("p" + std::to_string(i), static_cast<int64_t>(i));
    }
    return parameters;
  }

  std::string name_;
  rclcpp::Node::SharedPtr listener_;
  rclcpp::Subscription<ParameterEvent>::SharedPtr events_;
  rclcpp::executors::SingleThreadedExecutor executor_;
  std::thread spinner_;
  std::mutex mutex_;
  std::vector<ParameterEvent> received_;
};

TEST_F(TestDeclareParametersAtomically, one_event_and_one_callback_per_call)
{
  constexpr size_t count = 200;
  auto node = make_node();
  size_t on_set_calls = 0;
  size_t on_set_parameters = 0;
  size_t post_set_calls = 0;
  size_t pre_set_calls = 0;
  auto pre_handle = node->add_pre_set_parameters_callback(
    [&](std::vector<rclcpp::Parameter> &) {++pre_set_calls;});
  auto on_handle = node->add_on_set_parameters_callback(
    [&](const std::vector<rclcpp::Parameter> & parameters) {
      ++on_set_calls;
      on_set_parameters += parameters.size();
      rcl_interfaces::msg::SetParametersResult result;
      result.successful = true;
      return result;
    });
  auto post_handle = node->add_post_set_parameters_callback(
    [&](const std::vector<rclcpp::Parameter> &) {++post_set_calls;});

  auto values = node->declare_parameters_atomically("batch", numbered(count));
  ASSERT_EQ(values.size(), count);
  for (size_t i = 0; i < count; ++i) {
    EXPECT_EQ(values[i].get<int64_t>(), static_cast<int64_t>(i));
    EXPECT_EQ(node->get_parameter("batch.p" + std::to_string(i)).as_int(), static_cast<int64_t>(i));
  }
  EXPECT_EQ(on_set_calls, 1u);
  EXPECT_EQ(on_set_parameters, count);
  EXPECT_EQ(post_set_calls, 1u);
  EXPECT_EQ(pre_set_calls, 0u);

  auto events = settle();
  ASSERT_EQ(events.size(), 1u);
  ASSERT_EQ(events[0].new_parameters.size(), count);
  for (size_t i = 0; i < count; ++i) {
    EXPECT_EQ(events[0].new_parameters[i].name, "batch.p" + std::to_string(i));
    EXPECT_EQ(events[0].new_parameters[i].value.integer_value, static_cast<int64_t>(i));
  }

  // The existing declaration publishes one event and calls the callbacks per parameter.
  std::map<std::string, int64_t> map;
  for (size_t i = 0; i < count; ++i) {
    map["p" + std::to_string(i)] = static_cast<int64_t>(i);
  }
  node->declare_parameters("single", map);
  EXPECT_EQ(on_set_calls, 1u + count);
  EXPECT_EQ(post_set_calls, 1u + count);
  EXPECT_EQ(settle().size(), 1u + count);
}

TEST_F(TestDeclareParametersAtomically, failures_declare_nothing)
{
  auto node = make_node();
  node->declare_parameter("taken", 1);
  settle();
  const size_t events_before = events_count();

  auto expect_nothing_declared = [&node]() {
      EXPECT_FALSE(node->has_parameter("a"));
      EXPECT_FALSE(node->has_parameter("b"));
    };

  EXPECT_THROW(
    node->declare_parameters_atomically(
      "", {rclcpp::Parameter("a", 1), rclcpp::Parameter("", 2)}),
    rclcpp::exceptions::InvalidParametersException);
  expect_nothing_declared();

  EXPECT_THROW(
    node->declare_parameters_atomically(
      "", {rclcpp::Parameter("a", 1), rclcpp::Parameter("taken", 2)}),
    rclcpp::exceptions::ParameterAlreadyDeclaredException);
  expect_nothing_declared();

  EXPECT_THROW(
    node->declare_parameters_atomically(
      "", {rclcpp::Parameter("a", 1), rclcpp::Parameter("a", 2)}),
    rclcpp::exceptions::ParameterAlreadyDeclaredException);
  expect_nothing_declared();

  EXPECT_THROW(
    node->declare_parameters_atomically(
      "", {rclcpp::Parameter("a", 1), rclcpp::Parameter("b")}),
    rclcpp::exceptions::InvalidParameterTypeException);
  expect_nothing_declared();

  ParameterDescriptor range;
  range.integer_range.resize(1);
  range.integer_range[0].from_value = 0;
  range.integer_range[0].to_value = 10;
  EXPECT_THROW(
    node->declare_parameters_atomically(
      "", {{rclcpp::Parameter("a", 1), ParameterDescriptor{}},
        {rclcpp::Parameter("b", 11), range}}),
    rclcpp::exceptions::InvalidParameterValueException);
  expect_nothing_declared();

  size_t post_set_calls = 0;
  auto post_handle = node->add_post_set_parameters_callback(
    [&](const std::vector<rclcpp::Parameter> &) {++post_set_calls;});
  auto on_handle = node->add_on_set_parameters_callback(
    [](const std::vector<rclcpp::Parameter> & parameters) {
      rcl_interfaces::msg::SetParametersResult result;
      result.successful = parameters.size() == 1;
      result.reason = "only one at a time";
      return result;
    });
  EXPECT_THROW(
    node->declare_parameters_atomically(
      "", {rclcpp::Parameter("a", 1), rclcpp::Parameter("b", 2)}),
    rclcpp::exceptions::InvalidParameterValueException);
  expect_nothing_declared();
  EXPECT_EQ(post_set_calls, 0u);

  EXPECT_EQ(settle().size(), events_before);
}

TEST_F(TestDeclareParametersAtomically, overrides_apply_unless_ignored)
{
  auto node = make_node(
    rclcpp::NodeOptions()
    .append_parameter_override("ns.a", 5)
    .append_parameter_override("ns.b", "text")
    .append_parameter_override("ignored.a", 7)
    .append_parameter_override("c", "text"));

  auto values = node->declare_parameters_atomically(
    "ns", {rclcpp::Parameter("a", 1), rclcpp::Parameter("b", "default")});
  EXPECT_EQ(values[0].get<int64_t>(), 5);
  EXPECT_EQ(values[1].get<std::string>(), "text");

  values = node->declare_parameters_atomically("ignored", {rclcpp::Parameter("a", 1)}, true);
  EXPECT_EQ(values[0].get<int64_t>(), 1);

  EXPECT_THROW(
    node->declare_parameters_atomically("", {rclcpp::Parameter("c", 1)}),
    rclcpp::exceptions::InvalidParameterTypeException);
  EXPECT_FALSE(node->has_parameter("c"));

  auto events = settle();
  ASSERT_EQ(events.size(), 2u);
  ASSERT_EQ(events[0].new_parameters.size(), 2u);
  EXPECT_EQ(events[0].new_parameters[0].value.integer_value, 5);
  EXPECT_EQ(events[0].new_parameters[1].value.string_value, "text");
}

TEST_F(TestDeclareParametersAtomically, descriptors_are_kept)
{
  auto node = make_node();
  ParameterDescriptor read_only;
  read_only.read_only = true;
  read_only.description = "fixed";
  ParameterDescriptor dynamic;
  dynamic.dynamic_typing = true;

  auto values = node->declare_parameters_atomically(
    "", {{rclcpp::Parameter("fixed", 3), read_only},
      {rclcpp::Parameter("dynamic"), dynamic},
      {rclcpp::Parameter("dynamic_with_value", 2.5), dynamic}});
  ASSERT_EQ(values.size(), 3u);
  EXPECT_EQ(values[1].get_type(), rclcpp::PARAMETER_NOT_SET);

  auto descriptors = node->describe_parameters({"fixed", "dynamic", "dynamic_with_value"});
  EXPECT_TRUE(descriptors[0].read_only);
  EXPECT_EQ(descriptors[0].description, "fixed");
  EXPECT_EQ(descriptors[0].type, rcl_interfaces::msg::ParameterType::PARAMETER_INTEGER);
  EXPECT_TRUE(descriptors[1].dynamic_typing);
  EXPECT_TRUE(node->has_parameter("dynamic"));
  EXPECT_FALSE(node->set_parameter(rclcpp::Parameter("fixed", 4)).successful);
  EXPECT_TRUE(node->set_parameter(rclcpp::Parameter("dynamic_with_value", "now text")).successful);

  auto events = settle();
  ASSERT_GE(events.size(), 1u);
  // A parameter without value is declared but has nothing to announce.
  ASSERT_EQ(events[0].new_parameters.size(), 2u);
  EXPECT_EQ(events[0].new_parameters[0].name, "fixed");
  EXPECT_EQ(events[0].new_parameters[1].name, "dynamic_with_value");
}

TEST_F(TestDeclareParametersAtomically, empty_call_publishes_nothing)
{
  auto node = make_node();
  EXPECT_TRUE(node->declare_parameters_atomically("ns", std::vector<rclcpp::Parameter>{}).empty());
  EXPECT_EQ(settle().size(), 0u);
}
