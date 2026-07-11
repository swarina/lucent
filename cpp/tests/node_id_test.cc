#include "common/node_id.h"

#include <gtest/gtest.h>

namespace lucent {
namespace {

TEST(NodeIdentity, ParsesShard) {
  auto id = NodeIdentity::Parse("shard-2a");
  ASSERT_TRUE(id.has_value());
  EXPECT_EQ(id->role, Role::kShard);
  EXPECT_EQ(id->shard_id, 2);
  EXPECT_EQ(id->replica, 'a');
  EXPECT_EQ(id->id, "shard-2a");

  auto b = NodeIdentity::Parse("shard-10b");
  ASSERT_TRUE(b.has_value());
  EXPECT_EQ(b->shard_id, 10);
  EXPECT_EQ(b->replica, 'b');
}

TEST(NodeIdentity, ParsesOrdinalRoles) {
  auto coord = NodeIdentity::Parse("coord-0");
  ASSERT_TRUE(coord.has_value());
  EXPECT_EQ(coord->role, Role::kCoordinator);
  EXPECT_EQ(coord->ordinal, 0);

  EXPECT_EQ(NodeIdentity::Parse("embed-0")->role, Role::kEmbed);
  EXPECT_EQ(NodeIdentity::Parse("collector-0")->role, Role::kCollector);
  EXPECT_EQ(NodeIdentity::Parse("member-2")->role, Role::kMember);
  EXPECT_EQ(NodeIdentity::Parse("member-2")->ordinal, 2);
}

TEST(NodeIdentity, RejectsMalformed) {
  EXPECT_FALSE(NodeIdentity::Parse("").has_value());
  EXPECT_FALSE(NodeIdentity::Parse("shard-").has_value());
  EXPECT_FALSE(NodeIdentity::Parse("shard-a").has_value());
  EXPECT_FALSE(NodeIdentity::Parse("shard-2c").has_value());     // bad replica
  EXPECT_FALSE(NodeIdentity::Parse("shard-2").has_value());      // no replica
  EXPECT_FALSE(NodeIdentity::Parse("shard--1a").has_value());    // negative
  EXPECT_FALSE(NodeIdentity::Parse("shard-2a-x").has_value());   // trailing junk
  EXPECT_FALSE(NodeIdentity::Parse("coord-").has_value());
  EXPECT_FALSE(NodeIdentity::Parse("coord-x").has_value());
  EXPECT_FALSE(NodeIdentity::Parse("gateway-0").has_value());    // unknown role
}

}  // namespace
}  // namespace lucent
