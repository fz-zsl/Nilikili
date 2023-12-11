package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.RecommenderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class RecommenderServiceImpl implements RecommenderService {
	@Autowired
	private DataSource dataSource;

	/**
	 * Recommends a list of top 5 similar videos for a video.
	 * The similarity is defined as the number of users (in the database) who have watched both videos.
	 *
	 * @param bv the current video
	 * @return a list of video {@code bv}s
	 * @apiNote You may consider the following corner cases:
	 * <ul>
	 *   <li>{@code bv} is invalid (null or empty or not found)</li>
	 * </ul>
	 * If any of the corner case happened, {@code null} shall be returned.
	 */
	@Override
	public List<String> recommendNextVideo(String bv) {
		List<String> result = new ArrayList<>();
		// handle invalid bv
		if (bv == null || bv.isEmpty()) {
			log.error("Video not found or insufficient permission");
			return result;
		}
		String checkBvSQL = "select count(*) from video_info where bv = ? and active = true";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(checkBvSQL)) {
			stmt.setString(1, bv);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				if (rs.getInt(1) == 0) {
					log.error("Video not found or insufficient permission");
					return result;
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return result;
		}
		// get recommendations
		String recommendSQL = """
select count(bv) as cnt from (
	(select mid from user_watch_video where bv = ?) watchedBv join user_watch_video
		on watchedBv.mid = user_watch_video.mid) group by bv order by cnt desc limit 5
		""";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(recommendSQL)) {
			stmt.setString(1, bv);
			try (ResultSet rs = stmt.executeQuery()) {
				for (int i = 0; i < 5; ++i) {
					if (!rs.next()) break;
					result.add(rs.getString(1));
				}
				return result;
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return result;
		}
	}

	/**
	 * Recommends videos for anonymous users, based on the popularity.
	 * Evaluate the video's popularity from the following aspects:
	 * <ol>
	 *   <li>"like": the rate of watched users who also liked this video</li>
	 *   <li>"coin": the rate of watched users who also donated coin to this video</li>
	 *   <li>"fav": the rate of watched users who also collected this video</li>
	 *   <li>"danmu": the average number of danmus sent by one watched user</li>
	 *   <li>"finish": the average video watched percentage of one watched user</li>
	 * </ol>
	 * The recommendation score can be calculated as:
	 * <pre>
	 *   score = like + coin + fav + danmu + finish
	 * </pre>
	 *
	 * @param pageSize the page size, if there are less than {@code pageSize} videos, return all of them
	 * @param pageNum  the page number, starts from 1
	 * @return a list of video {@code bv}s, sorted by the recommendation score
	 * @implNote
	 * Though users can like/coin/favorite a video without watching it, the rates of these values should be clamped to 1.
	 * If no one has watched this video, all the five scores shall be 0.
	 * If the requested page is empty, return an empty list.
	 * @apiNote You may consider the following corner cases:
	 * <ul>
	 *   <li>{@code pageSize} and/or {@code pageNum} is invalid (any of them <= 0)</li>
	 * </ul>
	 * If any of the corner case happened, {@code null} shall be returned.
	 */
	@Override
	public List<String> generalRecommendations(int pageSize, int pageNum) {
		List<String> result = new ArrayList<>();
		// handle invalid page size and number
		if (pageSize <= 0 || pageNum <= 0) {
			log.error("Invalid page size or number");
			return result;
		}
		String recommendSQL = """
with auxCnt as (
	select count(*) as cnt from user_watch_video where bv = ?
)
select bv, (
    case
        when auxCnt.cnt == 0 then 0
        else ((
            (select count(*) from user_like_video where bv = ?) +
            (select count(*) from user_coin_video where bv = ?) +
            (select count(*) from user_fav_video where bv = ?) +
            (select count(*) from danmu_info where bv = ?) +
            (select sum(lastpos) from user_watch_video where bv = ?)
                / (select duration from video_info where bv = ?)
            ) / auxCnt.cnt)
    end) as score from video_info where active = true and (
		revMid is not null and publicTime <= now() -- visible to all
		or ? = 'SUPERUSER')  -- visible to super user only
limit ? offset ?;
		""";
		// get recommendations
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(recommendSQL)) {
			stmt.setInt(1, pageSize);
			stmt.setInt(2, (pageNum - 1) * pageSize);
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					result.add(rs.getString(1));
				}
				return result;
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return result;
		}
	}

	/**
	 * Recommends videos for a user, restricted on their interests.
	 * The user's interests are defined as the videos that the user's friend(s) have watched,
	 * filter out the videos that the user has already watched.
	 * Friend(s) of current user is/are the one(s) who is/are both the current user' follower and followee at the same time.
	 * Sort the videos by:
	 * <ol>
	 *   <li>The number of friends who have watched the video</li>
	 *   <li>The video owner's level</li>
	 *   <li>The video's public time (newer videos are preferred)</li>
	 * </ol>
	 *
	 * @param auth     the current user's authentication information to be recommended
	 * @param pageSize the page size, if there are less than {@code pageSize} videos, return all of them
	 * @param pageNum  the page number, starts from 1
	 * @return a list of video {@code bv}s
	 * @implNote
	 * If the current user's interest is empty, return {@link io.sustc.service.RecommenderService#generalRecommendations(int, int)}.
	 * If the requested page is empty, return an empty list
	 * @apiNote You may consider the following corner cases:
	 * <ul>
	 *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
	 *   <li>{@code pageSize} and/or {@code pageNum} is invalid (any of them <= 0)</li>
	 * </ul>
	 * If any of the corner case happened, {@code null} shall be returned.
	 */
	@Override
	public List<String> recommendVideosForUser(AuthInfo auth, int pageSize, int pageNum) {
		List<String> result = new ArrayList<>();
		// handle invalid auth
		long auth_mid = VerifyAuth.verifyAuth(auth);
		if (auth_mid == -1) {
			log.error("Auth verification failed.");
			return result;
		}
		// handle invalid page size and number
		if (pageSize <= 0 || pageNum <= 0) {
			log.error("Invalid page size or number");
			return result;
		}
		// get recommendations
		String recommendSQL = """
with friends as (
	select star_mid from user_follow where fan_mid = ?
	intersect
	select fan_mid from user_follow where star_mid = ?
)
select validBv.bv from (
	select bv, count(*) as cnt from user_watch_video
		where mid in (select * from friends) and bv not in (
			select bv from user_watch_video where mid = ?
		) group by bv) validBv
	join video_info on video_info.bv = validBv.bv
	join user_info on user_info.mid = video_info.ownMid
	where video_info.active = true and (
		revMid is not null and publicTime <= now() -- visible to all
		or ? = 'SUPERUSER')  -- visible to super user only
	order by cnt desc, level desc, publicTime desc
	limit ? offset ?;
		""";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(recommendSQL)) {
			stmt.setLong(1, auth_mid);
			stmt.setLong(2, auth_mid);
			stmt.setInt(3, pageSize);
			stmt.setInt(4, (pageNum - 1) * pageSize);
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					result.add(rs.getString(1));
				}
				return result;
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return result;
		}
	}

	/**
	 * Recommends friends for a user, based on their common followings.
	 * Find all users that are not currently followed by the user, and have at least one common following with the user.
	 * Sort the users by the number of common followings, if two users have the same number of common followings,
	 * sort them by their {@code level}.
	 *
	 * @param auth     the current user's authentication information to be recommended
	 * @param pageSize the page size, if there are less than {@code pageSize} users, return all of them
	 * @param pageNum  the page number, starts from 1
	 * @return a list of {@code mid}s of the recommended users
	 * @implNote If the requested page is empty, return an empty list
	 * @apiNote You may consider the following corner cases:
	 * <ul>
	 *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
	 *   <li>{@code pageSize} and/or {@code pageNum} is invalid (any of them <= 0)</li>
	 * </ul>
	 * If any of the corner case happened, {@code null} shall be returned.
	 */
	@Override
	public List<Long> recommendFriends(AuthInfo auth, int pageSize, int pageNum) {
		List<Long> result = new ArrayList<>();
		// handle invalid auth
		long auth_mid = VerifyAuth.verifyAuth(auth);
		if (auth_mid == -1) {
			log.error("Auth verification failed.");
			return result;
		}
		// handle invalid page size and number
		if (pageSize <= 0 || pageNum <= 0) {
			log.error("Invalid page size or number");
			return result;
		}
		// get recommendations
		String recommendSQL = """
with newFriends as (
	select fan_mid as mid, count(fan_mid) as cnt from
     (select star_mid from user_follow where fan_mid = ?) myFollowings
         join user_follow on myFollowings.star_mid = user_follow.star_mid
     where fan_mid not in (select star_mid from user_follow where fan_mid = ?) and fan_mid <> ?
     group by fan_mid
)
select user_info.mid
	from newFriends join user_info
		on newFriends.mid = user_info.mid
	order by cnt desc, level desc limit ? offset ?;
		""";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(recommendSQL)) {
			stmt.setLong(1, auth_mid);
			stmt.setLong(2, auth_mid);
			stmt.setLong(3, auth_mid);
			stmt.setInt(4, pageSize);
			stmt.setInt(5, (pageNum - 1) * pageSize);
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					result.add(rs.getLong(1));
				}
				return result;
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return result;
		}
	}
}
