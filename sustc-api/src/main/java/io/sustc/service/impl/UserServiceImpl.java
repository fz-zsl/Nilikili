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
import java.util.Arrays;

@Service
@Slf4j
public class UserServiceImpl implements UserService {
//	static int followCnt = 0;

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
	 *   <li>there is another user with same {@code qq} or {@code wechat} in {@code req}</li>
	 * </ul>
	 * If any of the corner case happened, {@code -1} shall be returned.
	 */
	@Override
	public long register(RegisterUserReq req) {
		if (req == null || req.getName() == null || req.getName().isEmpty() ||
				req.getSex() == null || req.getSex().toString().isEmpty() ||
				req.getPassword() == null || req.getPassword().isEmpty()) {
//			log.error("Invalid request.");
			return -1;
		}
		String userRegisterSQL = "select user_reg_sustc(?, ?, ?, ?, ?, ?, ?)";
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
//			log.error("SQL error: {}", e.getMessage());
			return -1;
		}
	}

	/**
	 * Deletes a user.
	 *
	 * @param auth indicates the current user
	 * @param mid  the user to be deleted
	 * @return operation success or not
	 * @apiNote You may consider the following corner cases:
	 * <ul>
	 *   <li>cannot find a user corresponding to the {@code mid}</li>
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
		String userDeleteSQL = "select user_del_sustc(?, ?, ?, ?, ?)";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(userDeleteSQL)) {
			stmt.setLong(1, auth.getMid());
			stmt.setString(2, auth.getPassword());
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
	 * @param auth        the authentication information of the follower
	 * @param followeeMid the user who will be followed
	 * @return the follow state after this operation
	 * @apiNote You may consider the following corner cases:
	 * <ul>
	 *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
	 *   <li>cannot find a user corresponding to the {@code followeeMid}</li>
	 * </ul>
	 * If any of the corner case happened, {@code false} shall be returned.
	 */
	@Override
	public boolean follow(AuthInfo auth, long followeeMid) {
//		++followCnt;
//		if (followCnt == 87) {
//			String alterSQL = "alter system set full_page_writes = on;";
//			try (Connection conn = dataSource.getConnection();
//			     PreparedStatement stmt = conn.prepareStatement(alterSQL)) {
//				stmt.execute();
//			} catch (SQLException e) {
//				log.error("SQL error: {}", e.getMessage());
//				return false;
//			}
//		}
		String userDeleteSQL = "select add_follow(?, ?, ?, ?, ?)";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(userDeleteSQL)) {
			stmt.setLong(1, auth.getMid());
			stmt.setString(2, auth.getPassword());
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
	 *   <li>cannot find a user corresponding to the {@code mid}</li>
	 * </ul>
	 * If any of the corner case happened, {@code null} shall be returned.
	 */
	@Override
	public UserInfoResp getUserInfo(long mid) {
		// handle invalid mid
		if (mid <= 0) {
//			log.error("Invalid mid.");
			return new UserInfoResp();
		}
		String userGetInfoSQL = "select * from get_user_info(?)";
		int coin;
		Long[] following;
		Long[] follower;
		String[] watched;
		String[] liked;
		String[] collected;
		String[] posted;
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(userGetInfoSQL)) {
			stmt.setLong(1, mid);
			ResultSet rs = stmt.executeQuery();
			if (!rs.next()) {
//				log.error("Cannot find user with mid {}.", mid);
				return new UserInfoResp();
			}
			coin = rs.getInt(1);
			if (rs.getArray(2) == null) following = new Long[0];
			else following = (Long[]) rs.getArray(2).getArray();
			if (rs.getArray(3) == null) follower = new Long[0];
			else follower = (Long[]) rs.getArray(3).getArray();
			if (rs.getArray(4) == null) watched = new String[0];
			else watched = (String[]) rs.getArray(4).getArray();
			if (rs.getArray(5) == null) liked = new String[0];
			else liked = (String[]) rs.getArray(5).getArray();
			if (rs.getArray(6) == null) collected = new String[0];
			else collected = (String[]) rs.getArray(6).getArray();
			if (rs.getArray(7) == null) posted = new String[0];
			else posted = (String[]) rs.getArray(7).getArray();
		} catch (SQLException e) {
//			log.error("SQL error: {}", e.getMessage());
			return new UserInfoResp();
		}
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
