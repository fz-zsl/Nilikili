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
	 *   <li>cannot find a video corresponding to the {@code bv}</li>
	 * </ul>
	 * If any of the corner case happened, {@code null} shall be returned.
	 */
	@Override
	public List<String> recommendNextVideo(String bv) {
		String recommendSQL = "select recommend_next_video(?)";
		List<String> result = new ArrayList<>();
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(recommendSQL)) {
			stmt.setString(1, bv);
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
		String recommendSQL = "select general_recommendations(?, ?)";
		List<String> result = new ArrayList<>();
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
		String recommendSQL = "select recommend_video_for_user(?, ?, ?, ?, ?, ?)";
		List<String> result = new ArrayList<>();
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(recommendSQL)) {
			stmt.setLong(1, auth.getMid());
			stmt.setString(2, auth.getPassword());
			stmt.setString(3, auth.getQq());
			stmt.setString(4, auth.getWechat());
			stmt.setInt(5, pageSize);
			stmt.setInt(6, (pageNum - 1) * pageSize);
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
		String recommendSQL = "select recommend_friends(?, ?, ?, ?, ?, ?)";
		List<Long> result = new ArrayList<>();
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(recommendSQL)) {
			stmt.setLong(1, auth.getMid());
			stmt.setString(2, auth.getPassword());
			stmt.setString(3, auth.getQq());
			stmt.setString(4, auth.getWechat());
			stmt.setInt(5, pageSize);
			stmt.setInt(6, (pageNum - 1) * pageSize);
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
