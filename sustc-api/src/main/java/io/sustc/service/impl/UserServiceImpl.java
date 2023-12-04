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
		// handle corner cases
		if (req.getName() == null || req.getName().isEmpty()) {
			log.error("Name is null or empty.");
			return -1;
		}
		if (req.getPassword() == null || req.getPassword().isEmpty()) {
			log.error("Password is null or empty.");
			return -1;
		}
		if (req.getSex() == null) {
			log.error("Sex is null.");
			return -1;
		}
		int month, day;
		if (req.getBirthday() != null && !req.getBirthday().isEmpty()) {
			String[] birthday = req.getBirthday().split("月");
			if (birthday.length != 2) {
				log.error("Birthday is invalid.");
				return -1;
			}
			try {
				month = Integer.parseInt(birthday[0]);
				day = Integer.parseInt(birthday[1].substring(0, birthday[1].length() - 1));
			} catch (NumberFormatException e) {
				log.error("Birthday is invalid.");
				return -1;
			}
			int [] days = {31,29,31,30,31,30,31,31,30,31,30,31};
			if (month < 1 || month > 12 || day < 1 || day > days[month]) {
				log.error("Birthday is invalid.");
				return -1;
			}
		}
		// duplicate check (name, qq, WeChat)
		String dupCheckSQL = "select count(*) from user_info where name = ? or qqid = ? or wxid = ?";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(dupCheckSQL)) {
			stmt.setString(1, req.getName());
			stmt.setString(2, req.getQq());
			stmt.setString(3, req.getWechat());
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				if (rs.getInt(1) != 0) {
					log.error("Duplicate name, qq or wechat.");
					return -1;
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return -1;
		}
		// user registration in SQL
		String registrationSQL = """
insert into user_info (name, sex, birthday, sign, identity, pwd, qqid, wxid)
	values (?, ?, to_date(?, 'YYYY-MM-DD'), ?, ?, ?, ?, ?);
		""";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(registrationSQL)) {
			stmt.setString(1, req.getName());
			stmt.setString(2, req.getSex().toString());
			stmt.setString(3, req.getBirthday());
			stmt.setString(4, req.getSign());
			stmt.setString(5, "USER");
			stmt.setString(6, EncryptSHA256.encrypt(req.getPassword()));
			stmt.setString(7, req.getQq());
			stmt.setString(8, req.getWechat());
			stmt.executeUpdate();
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return -1;
		}
		// get mid
		String getMidSQL = "select mid from user_info where name = ?";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(getMidSQL)) {
			stmt.setString(1, req.getName());
			try (ResultSet rs = stmt.executeQuery()) {
				if (rs.next()) {
					return rs.getLong("mid");
				}
				else {
					log.error("User not found.");
					return -1;
				}
			}
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
		// handle invalid mid
		if (mid <= 0) {
			log.error("Invalid mid.");
			return false;
		}
		String getMidSQL = "select count(*) as cnt from user_info where mid = ? and active = true";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(getMidSQL)) {
			stmt.setLong(1, mid);
			ResultSet rs = stmt.executeQuery();
			rs.next();
			if (rs.getInt("cnt") == 0) {
				log.error("User not found.");
				return false;
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		// handle invalid auth
		long auth_mid = VerifyAuth.verifyAuth(auth);
		if (auth_mid == -1) {
			log.error("Invalid auth.");
			return false;
		}
		// identity check
		String getIdentitySQL = "select identity from user_info where mid = ? and active = true";
		String auth_Identity;
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(getIdentitySQL)) {
			stmt.setLong(1, auth_mid);
			try (ResultSet rs = stmt.executeQuery()) {
				auth_Identity = rs.getString("identity");
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		if (auth_Identity.equals("USER") && auth_mid != mid) {
			log.error("A user can only delete his/her own account.");
			return false;
		}
		if (auth_Identity.equals("SUPERUSER")) {
			String checkDeletedIdentity = "select identity from user_info where mid = ? and active = true";
			String deletedIdentity;
			try (Connection conn = dataSource.getConnection();
			     PreparedStatement stmt = conn.prepareStatement(checkDeletedIdentity)) {
				stmt.setLong(1, mid);
				try (ResultSet rs = stmt.executeQuery()) {
					deletedIdentity = rs.getString("identity");
				}
			} catch (SQLException e) {
				log.error("SQL error: {}", e.getMessage());
				return false;
			}
			if (deletedIdentity.equals("SUPERUSER") && auth_mid != mid) {
				log.error("A super user cannot delete a superuser other than himself/herself.");
				return false;
			}
		}
		// account deletion in SQL
		String deleteAccountSQL = "update user_info set active = false where mid = ?";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(deleteAccountSQL)) {
			stmt.setLong(1, mid);
			stmt.executeUpdate();
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		// cascade delete videos
		String deleteVideoSQL = "update video_info set active = false where ownMid = ?";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(deleteVideoSQL)) {
			stmt.setLong(1, mid);
			stmt.executeUpdate();
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		// cascade delete danmu sent by the user
		String deleteDanmuSQL = "update danmu_info set active = false where senderMid = ?";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(deleteDanmuSQL)) {
			stmt.setLong(1, mid);
			stmt.executeUpdate();
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		// cascade delete danmu on user's video
		deleteDanmuSQL = """
update danmu_info set active = false where bv in (
	select bv from video_info where ownMid = ?
)
		""";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(deleteDanmuSQL)) {
			stmt.setLong(1, mid);
			stmt.executeUpdate();
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		return true;
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
		// handle invalid auth
		long auth_mid = VerifyAuth.verifyAuth(auth);
		if (auth_mid == -1) {
			log.error("Invalid auth.");
			return false;
		}
		// handle invalid followeeMid
		if (followeeMid <= 0) {
			log.error("Invalid followeeMid.");
			return false;
		}
		String getMidSQL = "select count(*) as cnt from user_info where mid = ? and active = true";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(getMidSQL)) {
			stmt.setLong(1, followeeMid);
			ResultSet rs = stmt.executeQuery();
			rs.next();
			if (rs.getInt("cnt") == 0) {
				log.error("Followee not found.");
				return false;
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		// follow/unfollow in SQL
		String followSQL = "insert into user_follow (star_mid, fan_mid) values (?, ?)";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(followSQL)) {
			stmt.setLong(1, followeeMid);
			stmt.setLong(2, auth_mid);
			stmt.executeUpdate();
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		return true;
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
		String getMidSQL = "select count(*) from user_info where mid = ? and active = true";
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
		String getCoinSQL = "select coin from user_info where mid = ?";
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
select star_mid
	from user_follow join user_info on user_follow.star_mid = user_info.mid
	where fan_mid = ? and active = true
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
select fan_mid
	from user_follow join user_info on user_follow.fan_mid = user_info.mid
	where star_mid = ? and active = true
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
select bv
	from user_watch_video join video_info on user_watch_video.bv = video_info.bv
	where mid = ? and revMid is not null and publicTime <= now() and active = true
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
select bv
	from user_like_video join video_info on user_like_video.bv = video_info.bv
	where mid = ? and revMid is not null and publicTime <= now() and active = true
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
select bv
	from user_fav_video join video_info on user_fav_video.bv = video_info.bv
	where mid = ? and revMid is not null and publicTime <= now() and active = true
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
select bv from video_info
	where ownMid = ? and revMid is not null and publicTime <= now() and active = true
		"""; // TODO: shall we display invisible videos?
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
