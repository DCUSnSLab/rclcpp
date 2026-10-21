// Copyright 2017 Open Source Robotics Foundation, Inc.
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
#include <string>
#include <memory>
#include <utility>
#include <vector>

#include "rclcpp/exceptions.hpp"
#include "rclcpp/rclcpp.hpp"

#include "../mocking_utils/patch.hpp"
#include "../utils/rclcpp_gtest_macros.hpp"

#include "rcl_interfaces/srv/list_parameters.hpp"
#include "test_msgs/srv/basic_types.hpp"
#include "test_msgs/srv/empty.hpp"
#include "test_msgs/srv/empty.h"

using namespace std::chrono_literals;

class TestService : public ::testing::Test
{
protected:
  static void SetUpTestCase()
  {
    rclcpp::init(0, nullptr);
  }

  static void TearDownTestCase()
  {
    rclcpp::shutdown();
  }

  void SetUp()
  {
    node = std::make_shared<rclcpp::Node>("my_node", "/ns");
  }

  void TearDown()
  {
    node.reset();
  }

  rclcpp::Node::SharedPtr node;
};

class TestServiceSub : public ::testing::Test
{
protected:
  static void SetUpTestCase()
  {
    rclcpp::init(0, nullptr);
  }

  static void TearDownTestCase()
  {
    rclcpp::shutdown();
  }

  void SetUp()
  {
    node = std::make_shared<rclcpp::Node>("my_node", "/ns");
    subnode = node->create_sub_node("sub_ns");
  }

  void TearDown()
  {
    node.reset();
  }

  rclcpp::Node::SharedPtr node;
  rclcpp::Node::SharedPtr subnode;
};

/*
   Testing service construction and destruction.
 */
TEST_F(TestService, construction_and_destruction) {
  auto callback = [](
    const rcl_interfaces::srv::ListParameters::Request::SharedPtr,
    rcl_interfaces::srv::ListParameters::Response::SharedPtr) {};
  {
    auto service = node->create_service<rcl_interfaces::srv::ListParameters>("service", callback);
    EXPECT_NE(nullptr, service->get_service_handle());
    const rclcpp::ServiceBase * const_service_base = service.get();
    EXPECT_NE(nullptr, const_service_base->get_service_handle());
  }

  {
    ASSERT_THROW(
    {
      auto service = node->create_service<rcl_interfaces::srv::ListParameters>(
        "invalid_service?", callback);
    }, rclcpp::exceptions::InvalidServiceNameError);
  }
}

/*
   Testing service construction and destruction for subnodes.
 */
TEST_F(TestServiceSub, construction_and_destruction) {
  auto callback = [](
    const rcl_interfaces::srv::ListParameters::Request::SharedPtr,
    rcl_interfaces::srv::ListParameters::Response::SharedPtr) {};
  {
    auto service = subnode->create_service<rcl_interfaces::srv::ListParameters>(
      "service", callback);
    EXPECT_STREQ(service->get_service_name(), "/ns/sub_ns/service");
  }

  {
    ASSERT_THROW(
    {
      auto service = subnode->create_service<rcl_interfaces::srv::ListParameters>(
        "invalid_service?", callback);
    }, rclcpp::exceptions::InvalidServiceNameError);
  }
}

TEST_F(TestService, construction_and_destruction_rcl_errors) {
  auto callback = [](
    const test_msgs::srv::Empty::Request::SharedPtr, test_msgs::srv::Empty::Response::SharedPtr) {};

  {
    auto mock = mocking_utils::patch_and_return("lib:rclcpp", rcl_service_init, RCL_RET_ERROR);
    // reset() isn't necessary for this exception, it just avoids unused return value warning
    EXPECT_THROW(
      node->create_service<test_msgs::srv::Empty>("service", callback).reset(),
      rclcpp::exceptions::RCLError);
  }
  {
    // reset() is required for this one
    auto mock = mocking_utils::patch_and_return("lib:rclcpp", rcl_service_fini, RCL_RET_ERROR);
    EXPECT_NO_THROW(node->create_service<test_msgs::srv::Empty>("service", callback).reset());
  }
}

/* Testing basic getters */
TEST_F(TestService, basic_public_getters) {
  auto callback = [](
    const rcl_interfaces::srv::ListParameters::Request::SharedPtr,
    rcl_interfaces::srv::ListParameters::Response::SharedPtr) {};
  auto service = node->create_service<rcl_interfaces::srv::ListParameters>("service", callback);
  EXPECT_STREQ(service->get_service_name(), "/ns/service");
  std::shared_ptr<rcl_service_t> service_handle = service->get_service_handle();
  EXPECT_NE(nullptr, service_handle);

  {
    // Create a extern defined const service
    auto node_handle_int = rclcpp::Node::make_shared("base_node");
    rcl_service_t service_handle = rcl_get_zero_initialized_service();
    rcl_service_options_t service_options = rcl_service_get_default_options();
    const rosidl_service_type_support_t * ts =
      rosidl_typesupport_cpp::get_service_type_support_handle<test_msgs::srv::Empty>();
    rcl_ret_t ret = rcl_service_init(
      &service_handle,
      node_handle_int->get_node_base_interface()->get_rcl_node_handle(),
      ts, "base_node_service", &service_options);
    if (ret != RCL_RET_OK) {
      FAIL();
      return;
    }
    rclcpp::AnyServiceCallback<test_msgs::srv::Empty> cb;
    const rclcpp::Service<test_msgs::srv::Empty> base(
      node_handle_int->get_node_base_interface()->get_shared_rcl_node_handle(),
      &service_handle, cb);
    // Use get_service_handle specific to const service
    std::shared_ptr<const rcl_service_t> const_service_handle = base.get_service_handle();
    EXPECT_NE(nullptr, const_service_handle);

    EXPECT_EQ(
      RCL_RET_OK, rcl_service_fini(
        &service_handle,
        node_handle_int->get_node_base_interface()->get_rcl_node_handle()));
  }
}

TEST_F(TestService, take_request) {
  auto callback = [](
    const test_msgs::srv::Empty::Request::SharedPtr, test_msgs::srv::Empty::Response::SharedPtr) {};
  auto server = node->create_service<test_msgs::srv::Empty>("service", callback);
  {
    auto request_id = server->create_request_header();
    test_msgs::srv::Empty::Request request;
    auto mock = mocking_utils::patch_and_return(
      "lib:rclcpp", rcl_take_request, RCL_RET_OK);
    EXPECT_TRUE(server->take_request(request, *request_id.get()));
  }
  {
    auto request_id = server->create_request_header();
    test_msgs::srv::Empty::Request request;
    auto mock = mocking_utils::patch_and_return(
      "lib:rclcpp", rcl_take_request, RCL_RET_SERVICE_TAKE_FAILED);
    EXPECT_FALSE(server->take_request(request, *request_id.get()));
  }
  {
    auto request_id = server->create_request_header();
    test_msgs::srv::Empty::Request request;
    auto mock = mocking_utils::patch_and_return(
      "lib:rclcpp", rcl_take_request, RCL_RET_ERROR);
    EXPECT_THROW(server->take_request(request, *request_id.get()), rclcpp::exceptions::RCLError);
  }
}

TEST_F(TestService, send_response) {
  auto callback = [](
    const test_msgs::srv::Empty::Request::SharedPtr, test_msgs::srv::Empty::Response::SharedPtr) {};
  auto server = node->create_service<test_msgs::srv::Empty>("service", callback);

  {
    auto request_id = server->create_request_header();
    test_msgs::srv::Empty::Response response;
    auto mock = mocking_utils::patch_and_return("lib:rclcpp", rcl_send_response, RCL_RET_OK);
    EXPECT_NO_THROW(server->send_response(*request_id.get(), response));
  }

  {
    auto request_id = server->create_request_header();
    test_msgs::srv::Empty::Response response;
    auto mock = mocking_utils::patch_and_return("lib:rclcpp", rcl_send_response, RCL_RET_ERROR);
    EXPECT_THROW(
      server->send_response(*request_id.get(), response),
      rclcpp::exceptions::RCLError);
  }
}

/*
   Testing on_new_request callbacks.
 */
TEST_F(TestService, on_new_request_callback) {
  auto server_callback = [](
    const test_msgs::srv::Empty::Request::SharedPtr,
    test_msgs::srv::Empty::Response::SharedPtr) {FAIL();};
  rclcpp::ServicesQoS service_qos;
  service_qos.keep_last(3);
  auto server = node->create_service<test_msgs::srv::Empty>(
    "~/test_service", server_callback, service_qos);

  std::atomic<size_t> c1 {0};
  auto increase_c1_cb = [&c1](size_t count_msgs) {c1 += count_msgs;};
  server->set_on_new_request_callback(increase_c1_cb);

  auto client = node->create_client<test_msgs::srv::Empty>(
    "~/test_service", service_qos);
  {
    auto request = std::make_shared<test_msgs::srv::Empty::Request>();
    client->async_send_request(request);
  }

  auto start = std::chrono::steady_clock::now();
  do {
    std::this_thread::sleep_for(100ms);
  } while (c1 == 0 && std::chrono::steady_clock::now() - start < 10s);

  EXPECT_EQ(c1.load(), 1u);

  std::atomic<size_t> c2 {0};
  auto increase_c2_cb = [&c2](size_t count_msgs) {c2 += count_msgs;};
  server->set_on_new_request_callback(increase_c2_cb);

  {
    auto request = std::make_shared<test_msgs::srv::Empty::Request>();
    client->async_send_request(request);
  }

  start = std::chrono::steady_clock::now();
  do {
    std::this_thread::sleep_for(100ms);
  } while (c2 == 0 && std::chrono::steady_clock::now() - start < 10s);

  EXPECT_EQ(c1.load(), 1u);
  EXPECT_EQ(c2.load(), 1u);

  server->clear_on_new_request_callback();

  {
    auto request = std::make_shared<test_msgs::srv::Empty::Request>();
    client->async_send_request(request);
    client->async_send_request(request);
    client->async_send_request(request);
  }

  std::atomic<size_t> c3 {0};
  auto increase_c3_cb = [&c3](size_t count_msgs) {c3 += count_msgs;};
  server->set_on_new_request_callback(increase_c3_cb);

  start = std::chrono::steady_clock::now();
  do {
    std::this_thread::sleep_for(100ms);
  } while (c3 < 3 && std::chrono::steady_clock::now() - start < 10s);

  EXPECT_EQ(c1.load(), 1u);
  EXPECT_EQ(c2.load(), 1u);
  EXPECT_EQ(c3.load(), 3u);

  std::function<void(size_t)> invalid_cb = nullptr;
  EXPECT_THROW(server->set_on_new_request_callback(invalid_cb), std::invalid_argument);
}

TEST_F(TestService, rcl_service_response_publisher_get_actual_qos_error) {
  auto mock = mocking_utils::patch_and_return(
    "lib:rclcpp", rcl_service_response_publisher_get_actual_qos, nullptr);
  auto callback = [](
    const test_msgs::srv::Empty::Request::SharedPtr, test_msgs::srv::Empty::Response::SharedPtr) {};
  auto server = node->create_service<test_msgs::srv::Empty>("service", callback);
  RCLCPP_EXPECT_THROW_EQ(
    server->get_response_publisher_actual_qos(),
    std::runtime_error("failed to get service's response publisher qos settings: error not set"));
}

TEST_F(TestService, rcl_service_request_subscription_get_actual_qos_error) {
  auto mock = mocking_utils::patch_and_return(
    "lib:rclcpp", rcl_service_request_subscription_get_actual_qos, nullptr);
  auto callback = [](
    const test_msgs::srv::Empty::Request::SharedPtr, test_msgs::srv::Empty::Response::SharedPtr) {};
  auto server = node->create_service<test_msgs::srv::Empty>("service", callback);
  RCLCPP_EXPECT_THROW_EQ(
    server->get_request_subscription_actual_qos(),
    std::runtime_error("failed to get service's request subscription qos settings: error not set"));
}


TEST_F(TestService, server_qos) {
  rclcpp::ServicesQoS qos_profile;
  qos_profile.liveliness(rclcpp::LivelinessPolicy::Automatic);
  rclcpp::Duration duration(std::chrono::milliseconds(1));
  qos_profile.deadline(duration);
  qos_profile.lifespan(duration);
  qos_profile.liveliness_lease_duration(duration);

  auto callback = [](
    const test_msgs::srv::Empty::Request::SharedPtr, test_msgs::srv::Empty::Response::SharedPtr) {};

  auto server = node->create_service<test_msgs::srv::Empty>("service", callback, qos_profile);
  auto rs_qos = server->get_request_subscription_actual_qos();
  auto rp_qos = server->get_response_publisher_actual_qos();

  EXPECT_EQ(qos_profile, rp_qos);
  // Lifespan has no meaning for subscription/readers
  rs_qos.lifespan(qos_profile.lifespan());
  EXPECT_EQ(qos_profile, rs_qos);
}

TEST_F(TestService, server_qos_depth) {
  uint64_t server_cb_count_ = 0;
  auto server_callback = [&](
    const test_msgs::srv::Empty::Request::SharedPtr,
    test_msgs::srv::Empty::Response::SharedPtr) {server_cb_count_++;};

  auto server_node = std::make_shared<rclcpp::Node>("server_node", "/ns");

  rclcpp::QoS server_qos_profile(2);

  auto server = server_node->create_service<test_msgs::srv::Empty>(
    "test_qos_depth", std::move(server_callback), server_qos_profile);

  rclcpp::QoS client_qos_profile(rclcpp::QoSInitialization::from_rmw(rmw_qos_profile_default));
  auto client = node->create_client<test_msgs::srv::Empty>("test_qos_depth", client_qos_profile);

  ::testing::AssertionResult request_result = ::testing::AssertionSuccess();
  auto request = std::make_shared<test_msgs::srv::Empty::Request>();

  auto client_callback = [&request_result](
    rclcpp::Client<test_msgs::srv::Empty>::SharedFuture future_response) {
      if (nullptr == future_response.get()) {
        request_result = ::testing::AssertionFailure() << "Future response was null";
      }
    };

  uint64_t client_requests = 5;
  for (uint64_t i = 0; i < client_requests; i++) {
    client->async_send_request(request, client_callback);
    std::this_thread::sleep_for(10ms);
  }

  auto start = std::chrono::steady_clock::now();
  while ((server_cb_count_ < server_qos_profile.depth()) &&
    (std::chrono::steady_clock::now() - start) < 1s)
  {
    rclcpp::spin_some(server_node);
    std::this_thread::sleep_for(1ms);
  }

  // Spin an extra time to check if server QoS depth has been ignored,
  // so more server responses might be processed than expected.
  rclcpp::spin_some(server_node);

  EXPECT_EQ(server_cb_count_, server_qos_profile.depth());
}

TEST_F(TestService, responses_start_from_defaults) {
  using S = test_msgs::srv::BasicTypes;
  std::vector<S::Request::SharedPtr> requests;
  auto server = node->create_service<S>(
    "fresh_responses", [&](S::Request::SharedPtr request, S::Response::SharedPtr response) {
      if (request->int64_value == 0) {
        response->string_value = request->string_value;
      }
      response->int64_value = request->int64_value;
      requests.push_back(request);
    });
  auto client = node->create_client<S>("fresh_responses");
  ASSERT_TRUE(client->wait_for_service(5s));
  auto call = [&](int64_t value) {
      auto request = std::make_shared<S::Request>();
      request->int64_value = value;
      request->string_value = std::string(4096, static_cast<char>('a' + value));
      auto future = client->async_send_request(request);
      EXPECT_EQ(
        rclcpp::FutureReturnCode::SUCCESS, rclcpp::spin_until_future_complete(node, future, 5s));
      return future.get();
    };
  EXPECT_EQ(std::string(4096, 'a'), call(0)->string_value);
  auto kept = call(1);
  EXPECT_EQ(1, kept->int64_value);
  EXPECT_EQ("", kept->string_value);
  EXPECT_EQ(std::string(4096, 'a'), call(0)->string_value);
  EXPECT_EQ(1, kept->int64_value);
  EXPECT_EQ("", kept->string_value);
  ASSERT_EQ(3u, requests.size());
  EXPECT_EQ(std::string(4096, 'a'), requests[0]->string_value);
  EXPECT_EQ(std::string(4096, 'b'), requests[1]->string_value);
  EXPECT_EQ(1, requests[1]->int64_value);
}

TEST_F(TestService, deferred_and_retained_responses) {
  using S = test_msgs::srv::BasicTypes;
  rclcpp::Service<S>::SharedPtr server;
  std::vector<std::pair<rmw_request_id_t, S::Request::SharedPtr>> pending;
  server = node->create_service<S>(
    "deferred_responses", [&](
      std::shared_ptr<rmw_request_id_t> header, S::Request::SharedPtr request) {
      pending.emplace_back(*header, request);
    });
  auto client = node->create_client<S>("deferred_responses");
  ASSERT_TRUE(client->wait_for_service(5s));
  std::vector<rclcpp::Client<S>::SharedFuture> futures;
  for (int64_t value = 0; value < 2; ++value) {
    auto request = std::make_shared<S::Request>();
    request->int64_value = value;
    futures.push_back(client->async_send_request(request).future.share());
  }
  auto start = std::chrono::steady_clock::now();
  while (pending.size() < 2 && std::chrono::steady_clock::now() - start < 5s) {
    rclcpp::spin_some(node);
  }
  ASSERT_EQ(2u, pending.size());
  S::Response response;
  for (auto it = pending.rbegin(); it != pending.rend(); ++it) {
    response.int64_value = it->second->int64_value + 100;
    response.string_value = std::string(2048, 'z');
    server->send_response(it->first, response);
  }
  response.string_value = "changed";
  for (auto & future : futures) {
    ASSERT_EQ(
      rclcpp::FutureReturnCode::SUCCESS, rclcpp::spin_until_future_complete(node, future, 5s));
  }
  EXPECT_EQ(100, futures[0].get()->int64_value);
  EXPECT_EQ(101, futures[1].get()->int64_value);
  EXPECT_EQ(std::string(2048, 'z'), futures[0].get()->string_value);
}
