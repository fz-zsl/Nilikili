package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.RegisterUserReq;
import io.sustc.dto.UserInfoResp;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;

@Service
@Slf4j
public class UserServiceImpl implements UserService {
	@Autowired
	private DataSource dataSource;

	/**
	 * Registers a new user.
	 * {@code password} is a mandatory field, while {@code qq} and {@code wechat} are optional
	 * <a href="https://openid.net/developers/how-connect-works/">OIDC</a> fields.
	 *
	 * @param req information of the new user
	 * @return the new user's {@code mid}
	 * @apiNote You may consider the following corner cases:
	 * <ul>
	 *   <li>{@code password} or {@code name} or {@code sex} in {@code req} is null or empty</li>
	 *   <li>{@code birthday} in {@code req} is valid (not null nor empty) while it's not a birthday (X月X日)</li>
	 *   <li>there is another user with same {@code name} or {@code qq} or {@code wechat} in {@code req}</li>
	 * </ul>
	 * If any of the corner case happened, {@code -1} shall be returned.
	 */
	@Override
	public long register(RegisterUserReq req) {
		String userRegisterSQL = "select user_reg(?, ?, ?, ?, ?, ?, ?)";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(userRegisterSQL)) {
			stmt.setString(1, req.getName());
			stmt.setString(2, req.getSex().toString());
			stmt.setString(3, req.getBirthday());
			stmt.setString(4, req.getSign());
			stmt.setString(5, req.getPassword());
			stmt.setString(6, req.getQq());
			stmt.setString(7, req.getWechat());
			ResultSet rs = stmt.executeQuery();
			rs.next();
			return rs.getLong(1);
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return -1;
		}
	}

	/**
	 * Deletes a user.
	 *
	 * @param auth indicates the current user
	 * @param mid  the user's {@code mid} to be deleted
	 * @return operation success or not
	 * @apiNote You may consider the following corner cases:
	 * <ul>
	 *   <li>{@code mid} is invalid (<= 0 or do not exist)</li>
	 *   <li>the {@code auth} is invalid
	 *     <ul>
	 *       <li>both {@code qq} and {@code wechat} are non-empty while they do not correspond to same user</li>
	 *       <li>{@code mid} is invalid while {@code qq} and {@code wechat} are both invalid (empty or not found)</li>
	 *     </ul>
	 *   </li>
	 *   <li>the current user is a regular user while the {@code mid} is not his/hers</li>
	 *   <li>the current user is a super user while the {@code mid} is neither a regular user's {@code mid} nor his/hers</li>
	 * </ul>
	 * If any of the corner case happened, {@code false} shall be returned.
	 */
	@Override
	public boolean deleteAccount(AuthInfo auth, long mid) {
		String userDeleteSQL = "select user_del(?, ?, ?, ?, ?)";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(userDeleteSQL)) {
			stmt.setLong(1, auth.getMid());
			stmt.setString(2, auth.getQq());
			stmt.setString(3, auth.getQq());
			stmt.setString(4, auth.getWechat());
			stmt.setLong(5, mid);
			ResultSet rs = stmt.executeQuery();
			rs.next();
			return rs.getBoolean(1);
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
	}

		/**
	 * Follow the user with {@code mid}.
	 * If that user has already been followed, unfollow the user.
	 *
	 * @param auth		the authentication information of the follower
	 * @param followeeMid the user who will be followed
	 * @return operation success or not
	 * @apiNote You may consider the following corner cases:
	 * <ul>
	 *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
	 *   <li>{@code followeeMid} is invalid (<= 0 or not found)</li>
	 * </ul>
	 * If any of the corner case happened, {@code false} shall be returned.
	 */
	@Override
	public boolean follow(AuthInfo auth, long followeeMid) {
		String userDeleteSQL = "select add_follow(?, ?, ?, ?, ?)";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(userDeleteSQL)) {
			stmt.setLong(1, auth.getMid());
			stmt.setString(2, auth.getQq());
			stmt.setString(3, auth.getQq());
			stmt.setString(4, auth.getWechat());
			stmt.setLong(5, followeeMid);
			ResultSet rs = stmt.executeQuery();
			rs.next();
			return rs.getBoolean(1);
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
	}

	/**
	 * Gets the required information (in DTO) of a user.
	 *
	 * @param mid the user to be queried
	 * @return the personal information of given {@code mid}
	 * @apiNote You may consider the following corner cases:
	 * <ul>
	 *   <li>{@code mid} is invalid (<= 0 or not found)</li>
	 * </ul>
	 * If any of the corner case happened, {@code null} shall be returned.
	 */
	@Override
	public UserInfoResp getUserInfo(long mid) {
		// handle invalid mid
		if (mid <= 0) {
			log.error("Invalid mid.");
			return null;
		}
		String getMidSQL = "select count(*) from user_active where mid = ?";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(getMidSQL)) {
			stmt.setLong(1, mid);
			ResultSet rs = stmt.executeQuery();
			rs.next();
			if (rs.getInt(1) == 0) {
				log.error("User not found.");
				return null;
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return null;
		}
		// get user info in SQL
		// coin
		String getCoinSQL = "select coin from user_active where mid = ?";
		int coin;
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(getCoinSQL)) {
			stmt.setLong(1, mid);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				coin = rs.getInt("coin");
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return null;
		}
		// following
		String getFollowingSQL = """
select star_mid from user_follow
	where fan_mid = ? and star_mid in (select mid from user_active)
		""";
		ArrayList<Long> followingList = new ArrayList<>();
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(getFollowingSQL)) {
			stmt.setLong(1, mid);
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					followingList.add(rs.getLong("star_mid"));
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return null;
		}
		Long[] following = followingList.toArray(new Long[0]);
		// follower
		String getFollowerSQL = """
select fan_mid from user_follow
	where star_mid = ? and fan_mid in (select mid from user_active)
		""";
		ArrayList<Long> followerList = new ArrayList<>();
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(getFollowerSQL)) {
			stmt.setLong(1, mid);
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					followerList.add(rs.getLong("fan_mid"));
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return null;
		}
		Long [] follower = followerList.toArray(new Long[0]);
		// watched
		String getWatchedSQL = """
(select bv from user_watch_video where mid = ?)
intersect (select bv from video_active)
		""";
		ArrayList<String> watchedList = new ArrayList<>();
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(getWatchedSQL)) {
			stmt.setLong(1, mid);
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					watchedList.add(rs.getString("bv"));
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return null;
		}
		String [] watched = watchedList.toArray(new String[0]);
		// liked
		String getLikedSQL = """
(select bv from user_like_video where mid = ?)
intersect (select bv from video_active)
		""";
		ArrayList<String> likedList = new ArrayList<>();
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(getLikedSQL)) {
			stmt.setLong(1, mid);
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					likedList.add(rs.getString("bv"));
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return null;
		}
		String [] liked = likedList.toArray(new String[0]);
		// collected
		String getCollectedSQL = """
(select bv from user_fav_video where mid = ?)
intersect (select bv from video_active)
		""";
		ArrayList<String> collectedList = new ArrayList<>();
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(getCollectedSQL)) {
			stmt.setLong(1, mid);
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					collectedList.add(rs.getString("bv"));
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return null;
		}
		String [] collected = collectedList.toArray(new String[0]);
		// posted
		String getPostedSQL = """
(select bv from video_info where ownMid = ?)
intersect (select bv from video_active)
		""";
		ArrayList<String> postedList = new ArrayList<>();
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(getPostedSQL)) {
			stmt.setLong(1, mid);
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					postedList.add(rs.getString("bv"));
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return null;
		}
		String [] posted = postedList.toArray(new String[0]);
		return UserInfoResp.builder()
				.mid(mid)
				.coin(coin)
				.following(Arrays.stream(following).mapToLong(Long::longValue).toArray())
				.follower(Arrays.stream(follower).mapToLong(Long::longValue).toArray())
				.watched(watched)
				.liked(liked)
				.collected(collected)
				.posted(posted)
				.build();
	}
}
