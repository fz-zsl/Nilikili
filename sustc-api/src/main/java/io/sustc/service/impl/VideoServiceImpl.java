package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PostVideoReq;
import io.sustc.service.VideoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service
@Slf4j
public class VideoServiceImpl implements VideoService {
	@Autowired
	private DataSource dataSource;

	/**
	 * Posts a video. Its commit time shall be {@link LocalDateTime#now()}.
	 *
	 * @param auth the current user's authentication information
	 * @param req  the video's information
	 * @return the video's {@code bv}
	 * @apiNote You may consider the following corner cases:
	 * <ul>
	 *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
	 *   <li>{@code req} is invalid
	 *     <ul>
	 *       <li>{@code title} is null or empty</li>
	 *       <li>there is another video with same {@code title} and same user</li>
	 *       <li>{@code duration} is less than 10 (so that no chunk can be divided)</li>
	 *       <li>{@code publicTime} is earlier than {@link LocalDateTime#now()}</li>
	 *     </ul>
	 *   </li>
	 * </ul>
	 * If any of the corner case happened, {@code null} shall be returned.
	 */
	@Override
	public String postVideo(AuthInfo auth, PostVideoReq req) {
		String postVideoSQL = "select post_video(?, ?, ?, ?, ?, ?, ?, ?)";
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(postVideoSQL)) {
			stmt.setLong(1, auth.getMid());
			stmt.setString(2, auth.getPassword());
			stmt.setString(3, auth.getQq());
			stmt.setString(4, auth.getWechat());
			stmt.setString(5, req.getTitle());
			stmt.setString(6, req.getDescription());
			stmt.setFloat(7, req.getDuration());
			stmt.setTimestamp(8, req.getPublicTime());
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				return rs.getString(1);
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return null;
		}
	}

	/**
	 * Deletes a video.
	 * This operation can be performed by the video owner or a superuser.
	 * The coins of this video will not be returned to their donators.
	 *
	 * @param auth the current user's authentication information
	 * @param bv   the video's {@code bv}
	 * @return success or not
	 * @apiNote You may consider the following corner cases:
	 * <ul>
	 *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
	 *   <li>cannot find a video corresponding to the {@code bv}</li>
	 *   <li>{@code auth} is not the owner of the video nor a superuser</li>
	 * </ul>
	 * If any of the corner case happened, {@code false} shall be returned.
	 */
	@Override
	public boolean deleteVideo(AuthInfo auth, String bv) {
		String deleteVideoSQL = "select del_video(?, ?, ?, ?, ?)";
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(deleteVideoSQL)) {
			stmt.setLong(1, auth.getMid());
			stmt.setString(2, auth.getPassword());
			stmt.setString(3, auth.getQq());
			stmt.setString(4, auth.getWechat());
			stmt.setString(5, bv);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				return rs.getBoolean(1);
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
	}

	/**
	 * Updates the video's information.
	 * Only the owner of the video can update the video's information.
	 * If the video was reviewed before, a new review for the updated video is required.
	 * The duration shall not be modified and therefore the likes, favorites and danmus are not required to update.
	 *
	 * @param auth the current user's authentication information
	 * @param bv   the video's {@code bv}
	 * @param req  the new video information
	 * @return {@code true} if the video needs to be re-reviewed (was reviewed before), {@code false} otherwise
	 * @apiNote You may consider the following corner cases:
	 * <ul>
	 *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
	 *   <li>cannot find a video corresponding to the {@code bv}</li>
	 *   <li>{@code auth} is not the owner of the video</li>
	 *   <li>{@code req} is invalid, as stated in {@link io.sustc.service.VideoService#postVideo(AuthInfo, PostVideoReq)}</li>
	 *   <li>{@code duration} in {@code req} is changed compared to current one</li>
	 *   <li>{@code req} is not changed compared to current information</li>
	 * </ul>
	 * If any of the corner case happened, {@code false} shall be returned.
	 */
	@Override
	public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req) {
		String updateVideoInfoSQL = "select update_video(?, ?, ?, ?, ?, ?, ?, ?, ?)";
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(updateVideoInfoSQL)) {
			stmt.setLong(1, auth.getMid());
			stmt.setString(2, auth.getPassword());
			stmt.setString(3, auth.getQq());
			stmt.setString(4, auth.getWechat());
			stmt.setString(5, bv);
			stmt.setString(6, req.getTitle());
			stmt.setString(7, req.getDescription());
			stmt.setFloat(8, req.getDuration());
			stmt.setTimestamp(9, req.getPublicTime());
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				return rs.getBoolean(1);
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
	}

	/**
	 * Search the videos by keywords (split by space).
	 * You should try to match the keywords case-insensitively in the following fields:
	 * <ol>
	 *   <li>title</li>
	 *   <li>description</li>
	 *   <li>owner name</li>
	 * </ol>
	 * <p>
	 * Sort the results by the relevance (sum up the number of keywords matched in the three fields).
	 * <ul>
	 *   <li>If a keyword occurs multiple times, it should be counted more than once.</li>
	 *   <li>
	 *     A character in these fields can only be counted once for each keyword
	 *     but can be counted for different keywords.
	 *   </li>
	 *   <li>If two videos have the same relevance, sort them by the number of views.</li>
	 * </u
	 * <p>
	 * Examples:
	 * <ol>
	 *   <li>
	 *     If the title is "1122" and the keywords are "11 12",
	 *     then the relevance in the title is 2 (one for "11" and one for "12").
	 *   </li>
	 *   <li>
	 *     If the title is "111" and the keyword is "11",
	 *     then the relevance in the title is 1 (one for the occurrence of "11").
	 *   </li>
	 *   <li>
	 *     Consider a video with title "Java Tutorial", description "Basic to Advanced Java", owner name "John Doe".
	 *     If the search keywords are "Java Advanced",
	 *     then the relevance is 3 (one occurrence in the title and two in the description).
	 *   </li>
	 * </ol>
	 * <p>
	 * Unreviewed or unpublished videos are only visible to superusers or the video owner.
	 *
	 * @param auth     the current user's authentication information
	 * @param keywords the keywords to search, e.g. "sustech database final review"
	 * @param pageSize the page size, if there are less than {@code pageSize} videos, return all of them
	 * @param pageNum  the page number, starts from 1
	 * @return a list of video {@code bv}s
	 * @implNote If the requested page is empty, return an empty list
	 * @apiNote You may consider the following corner cases:
	 * <ul>
	 *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
	 *   <li>{@code keywords} is null or empty</li>
	 *   <li>{@code pageSize} and/or {@code pageNum} is invalid (any of them <= 0)</li>
	 * </ul>
	 * If any of the corner case happened, {@code null} shall be returned.
	 */
	@Override
	public List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum) {
		List<String> result = new ArrayList<>();
		String searchVideoSQL = "select bv from search_video(?, ?, ?, ?, ?, ?, ?)";
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(searchVideoSQL)) {
			stmt.setLong(1, auth.getMid());
			stmt.setString(2, auth.getPassword());
			stmt.setString(3, auth.getQq());
			stmt.setString(4, auth.getWechat());
			stmt.setString(5, keywords);
			stmt.setInt(6, pageSize);
			stmt.setInt(7, pageNum);
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					result.add(rs.getString(1));
				}
				return result;
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return null;
		}
	}

	/**
	 * Calculates the average view rate of a video.
	 * The view rate is defined as the user's view time divided by the video's duration.
	 *
	 * @param bv the video's {@code bv}
	 * @return the average view rate
	 * @apiNote You may consider the following corner cases:
	 * <ul>
	 *   <li>cannot find a video corresponding to the {@code bv}</li>
	 *   <li>no one has watched this video</li>
	 * </ul>
	 * If any of the corner case happened, {@code -1} shall be returned.
	 */
	@Override
	public double getAverageViewRate(String bv) {
		String getAverageViewRateSQL = "select get_avg_view_rate(?)";
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(getAverageViewRateSQL)) {
			stmt.setString(1, bv);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				return rs.getDouble(1);
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return -1;
		}
	}

	/**
	 * Gets the hotspot of a video.
	 * With splitting the video into 10-second chunks, hotspots are defined as chunks with the most danmus.
	 *
	 * @param bv the video's {@code bv}
	 * @return the index of hotspot chunks (start from 0)
	 * @apiNote You may consider the following corner cases:
	 * <ul>
	 *   <li>cannot find a video corresponding to the {@code bv}</li>
	 *   <li>no one has sent danmu on this video</li>
	 * </ul>
	 * If any of the corner case happened, an empty set shall be returned.
	 */
	@Override
	public Set<Integer> getHotspot(String bv) {
		Set<Integer> result = new HashSet<>();
		String getHotspotSQL = "select get_hotspot(?)";
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(getHotspotSQL)) {
			stmt.setString(1, bv);
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					result.add(rs.getInt(1));
				}
				return result;
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return new HashSet<>();
		}
	}

	/**
	 * Reviews a video by a superuser.
	 * If the video is already reviewed, do not modify the review info.
	 *
	 * @param auth the current user's authentication information
	 * @param bv   the video's {@code bv}
	 * @return {@code true} if the video is newly successfully reviewed, {@code false} otherwise
	 * @apiNote You may consider the following corner cases:
	 * <ul>
	 *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
	 *   <li>cannot find a video corresponding to the {@code bv}</li>
	 *   <li>{@code auth} is not a superuser or he/she is the owner</li>
	 *   <li>the video is already reviewed</li>
	 * </ul>
	 * If any of the corner case happened, {@code false} shall be returned.
	 */
	@Override
	public boolean reviewVideo(AuthInfo auth, String bv) {
		String revVideoSQL = "select rev_video(?, ?, ?, ?, ?)";
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(revVideoSQL)) {
			stmt.setLong(1, auth.getMid());
			stmt.setString(2, auth.getPassword());
			stmt.setString(3, auth.getQq());
			stmt.setString(4, auth.getWechat());
			stmt.setString(5, bv);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				return rs.getBoolean(1);
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
	}

	/**
	 * Donates one coin to the video. A user can at most donate one coin to a video.
	 * The user can only coin a video if he/she can search it ({@link io.sustc.service.VideoService#searchVideo(AuthInfo, String, int, int)}).
	 * It is not mandatory that the user shall watch the video first before he/she donates coin to it.
	 * If the current user donated a coin to this video successfully, he/she's coin number shall be reduced by 1.
	 * However, the coin number of the owner of the video shall NOT increase.
	 *
	 * @param auth the current user's authentication information
	 * @param bv   the video's {@code bv}
	 * @return whether a coin is successfully donated
	 * @implNote There is no way to earn coins in this project for simplicity
	 * @apiNote You may consider the following corner cases:
	 * <ul>
	 *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
	 *   <li>cannot find a video corresponding to the {@code bv}</li>
	 *   <li>the user cannot search this video or he/she is the owner</li>
	 *   <li>the user has no coin or has donated a coin to this video (user cannot withdraw coin donation)</li>
	 * </ul>
	 * If any of the corner case happened, {@code false} shall be returned.
	 */
	@Override
	public boolean coinVideo(AuthInfo auth, String bv) {
		String coinVideoSQL = "select coin_video(?, ?, ?, ?, ?)";
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(coinVideoSQL)) {
			stmt.setLong(1, auth.getMid());
			stmt.setString(2, auth.getPassword());
			stmt.setString(3, auth.getQq());
			stmt.setString(4, auth.getWechat());
			stmt.setString(5, bv);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				return rs.getBoolean(1);
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
	}

	/**
	 * Likes a video.
	 * The user can only like a video if he/she can search it ({@link io.sustc.service.VideoService#searchVideo(AuthInfo, String, int, int)}).
	 * If the user already liked the video, the operation will cancel the like.
	 * It is not mandatory that the user shall watch the video first before he/she likes to it.
	 *
	 * @param auth the current user's authentication information
	 * @param bv   the video's {@code bv}
	 * @return the like state of the user to this video after this operation
	 * @apiNote You may consider the following corner cases:
	 * <ul>
	 *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
	 *   <li>cannot find a video corresponding to the {@code bv}</li>
	 *   <li>the user cannot search this video or the user is the video owner</li>
	 * </ul>
	 * If any of the corner case happened, {@code false} shall be returned.
	 */
	@Override
	public boolean likeVideo(AuthInfo auth, String bv) {
		String likeVideoSQL = "select like_video(?, ?, ?, ?, ?)";
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(likeVideoSQL)) {
			stmt.setLong(1, auth.getMid());
			stmt.setString(2, auth.getPassword());
			stmt.setString(3, auth.getQq());
			stmt.setString(4, auth.getWechat());
			stmt.setString(5, bv);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				return rs.getBoolean(1);
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
	}

	/**
	 * Collects a video.
	 * The user can only collect a video if he/she can search it.
	 * If the user already collected the video, the operation will cancel the collection.
	 * It is not mandatory that the user shall watch the video first before he/she collects coin to it.
	 *
	 * @param auth the current user's authentication information
	 * @param bv   the video's {@code bv}
	 * @return the collect state of the user to this video after this operation
	 * @apiNote You may consider the following corner cases:
	 * <ul>
	 *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
	 *   <li>cannot find a video corresponding to the {@code bv}</li>
	 *   <li>the user cannot search this video or the user is the video owner</li>
	 * </ul>
	 * If any of the corner case happened, {@code false} shall be returned.
	 */
	@Override
	public boolean collectVideo(AuthInfo auth, String bv) {
		String collectVideoSQL = "select fav_video(?, ?, ?, ?, ?)";
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(collectVideoSQL)) {
			stmt.setLong(1, auth.getMid());
			stmt.setString(2, auth.getPassword());
			stmt.setString(3, auth.getQq());
			stmt.setString(4, auth.getWechat());
			stmt.setString(5, bv);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				return rs.getBoolean(1);
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
	}
}
