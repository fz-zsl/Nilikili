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
	 *	 <ul>
	 *	   <li>{@code title} is null or empty</li>
	 *	   <li>there is another video with same {@code title} and same user</li>
	 *	   <li>{@code duration} is less than 10 (so that no chunk can be divided)</li>
	 *	   <li>{@code publicTime} is earlier than {@link LocalDateTime#now()}</li>
	 *	 </ul>
	 *   </li>
	 * </ul>
	 * If any of the corner case happened, {@code null} shall be returned.
	 */
	@Override
	public String postVideo(AuthInfo auth, PostVideoReq req) {
		// handle invalid author
		long auth_mid = VerifyAuth.verifyAuth(auth);
		if (auth_mid == -1) {
			log.error("Auth verification failed.");
			return null;
		}
		if (!VerifyVideoReq.verifyVideoReq(req, auth_mid)) {
			log.error("Invalid PostVideoReq.");
			return null;
		}
		// insert video into video_info
		String insertVideoSQL = """
insert into video_info (bv, title, ownMid, commitTime, publicTime, duration, descr)
	values (generate_unique_bv(), ?, ?, ?, ?, ?, ?)
		""";
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(insertVideoSQL)) {
			stmt.setString(1, req.getTitle());
			stmt.setLong(2, auth_mid);
			stmt.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
			stmt.setTimestamp(4, req.getPublicTime());
			stmt.setFloat(5, req.getDuration());
			stmt.setString(6, req.getDescription());
			stmt.executeUpdate();
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return null;
		}
		// get bv of the video
		String getBvSQL = "select bv from video_info where title = ? and ownMid = ? and active = true";
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(getBvSQL)) {
			stmt.setString(1, req.getTitle());
			stmt.setLong(2, auth_mid);
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
	 *
	 * @param auth the current user's authentication information
	 * @param bv   the video's {@code bv}
	 * @return success or not
	 * @apiNote You may consider the following corner cases:
	 * <ul>
	 *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
	 *   <li>{@code bv} is invalid (null or empty or not found)</li>
	 *   <li>{@code auth} is not the owner of the video nor a superuser</li>
	 * </ul>
	 * If any of the corner case happened, {@code false} shall be returned.
	 */
	@Override
	public boolean deleteVideo(AuthInfo auth, String bv) {
		// handle invalid auth
		long auth_mid = VerifyAuth.verifyAuth(auth);
		if (auth_mid == -1) {
			log.error("Auth verification failed.");
			return false;
		}
		// handle invalid bv
		if (bv == null || bv.isEmpty()) {
			log.error("Video not found or insufficient permission");
			return false;
		}
		String checkBvSQL = "select count(*) from video_info where bv = ? and active = true";
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(checkBvSQL)) {
			stmt.setString(1, bv);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				if (rs.getInt(1) == 0) {
					log.error("Video not found or insufficient permission");
					return false;
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		// get auth identity
		String getAuthIdentitySQL = "select identity from user_info where mid = ?";
		String authIdentity;
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(getAuthIdentitySQL)) {
			stmt.setLong(1, auth_mid);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				authIdentity = rs.getString(1);
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		// get owner mid
		String getOwnerMidSQL = "select ownMid from video_info where bv = ?";
		long ownerMid;
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(getOwnerMidSQL)) {
			stmt.setString(1, bv);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				ownerMid = rs.getLong(1);
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		if (authIdentity.equals("USER") && auth_mid != ownerMid) {
			log.error("Video not found or insufficient permission");
		}
		// deletion in SQL
		String deletionSQL = "update video_info set active = false where bv = ?";
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(deletionSQL)) {
			stmt.setString(1, bv);
			stmt.executeUpdate();
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		// cascade delete danmu on user's video
		String deleteDanmuSQL = "update danmu_info set active = false where bv = ?";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(deleteDanmuSQL)) {
			stmt.setString(1, bv);
			stmt.executeUpdate();
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		return true;
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
	 *   <li>{@code bv} is invalid (null or empty or not found)</li>
	 *   <li>{@code auth} is not the owner of the video</li>
	 *   <li>{@code req} is invalid, as stated in {@link io.sustc.service.VideoService#postVideo(AuthInfo, PostVideoReq)}</li>
	 *   <li>{@code duration} in {@code req} is changed compared to current one</li>
	 *   <li>{@code req} is not changed compared to current information</li>
	 * </ul>
	 * If any of the corner case happened, {@code false} shall be returned.
	 */
	@Override
	public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req) {
		long auth_mid = VerifyAuth.verifyAuth(auth);
		if (auth_mid == -1) {
			log.error("Auth verification failed.");
			return false;
		}
		// handle invalid bv and get owner's mid
		if (bv == null || bv.isEmpty()) {
			log.error("Video not found or insufficient permission");
			return false;
		}
		String checkBvSQL = "select ownMid from video_info where bv = ? and active = true";
		long ownerMid;
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(checkBvSQL)) {
			stmt.setString(1, bv);
			try (ResultSet rs = stmt.executeQuery()) {
				if (rs.next()) {
					ownerMid = rs.getLong(1);
				}
				else {
					log.error("Modification failed (video not found or insufficient permission).");
					return false;
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		// auth is not the owner
		if (auth_mid != ownerMid) {
			log.error("Modification failed (video not found or insufficient permission).");
			return false;
		}
		// invalid req
		if (!VerifyVideoReq.verifyVideoReq(req, auth_mid)) {
			log.error("Invalid PostVideoReq.");
			return false;
		}
		// invalid information (duration and others)
		String getVideoInfoSQL = """
select (bv, title, descr, duration, publicTime)
	from video_info where bv = ? and active = true
		""";
		String prevTitle, prevDescr;
		float prevDuration;
		Timestamp prevPublicTime;
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(getVideoInfoSQL)) {
			stmt.setString(1, bv);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				prevTitle = rs.getString("title");
				prevDescr = rs.getString("descr");
				prevDuration = rs.getFloat("duration");
				prevPublicTime = rs.getTimestamp("publicTime");
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		// duration changes => invalid
		if (req.getDuration() != prevDuration) {
			log.error("Duration changes.");
			return false;
		}
		// nothing changes => invalid
		if (req.getTitle().equals(prevTitle)
				&& req.getDescription().equals(prevDescr)
				&& req.getPublicTime().equals(prevPublicTime)) {
			log.error("Nothing changes.");
			return false;
		}
		// update video info
		String updateVideoSQL = """
update video_info
	set title = ?, descr = ?, publicTime = ?
	where bv = ? and active = true
		""";
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(updateVideoSQL)) {
			stmt.setString(1, req.getTitle());
			stmt.setString(2, req.getDescription());
			stmt.setTimestamp(3, req.getPublicTime());
			stmt.setString(4, bv);
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		return true;
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
	 *	 A character in these fields can only be counted once for each keyword
	 *	 but can be counted for different keywords.
	 *   </li>
	 *   <li>If two videos have the same relevance, sort them by the number of views.</li>
	 * </u
	 * <p>
	 * Examples:
	 * <ol>
	 *   <li>
	 *	 If the title is "1122" and the keywords are "11 12",
	 *	 then the relevance in the title is 2 (one for "11" and one for "12").
	 *   </li>
	 *   <li>
	 *	 If the title is "111" and the keyword is "11",
	 *	 then the relevance in the title is 1 (one for the occurrence of "11").
	 *   </li>
	 *   <li>
	 *	 Consider a video with title "Java Tutorial", description "Basic to Advanced Java", owner name "John Doe".
	 *	 If the search keywords are "Java Advanced",
	 *	 then the relevance is 3 (one occurrence in the title and two in the description).
	 *   </li>
	 * </ol>
	 * <p>
	 * Unreviewed or unpublished videos are only visible to superusers or the video owner.
	 *
	 * @param auth	 the current user's authentication information
	 * @param keywords the keywords to search, e.g. "sustech database final review"
	 * @param pageSize the page size, if there are less than {@code pageSize} videos, return all of them
	 * @param pageNum  the page number, starts from 1
	 * @return a list of video {@code bv}s
	 * @implNote If the requested page is empty, return an empty list
	 * @apiNote You may consider the following corner cases:
	 * <ul>
	 *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
	 *   <li>{@code keywords} is invalid (null or empty)</li>
	 *   <li>{@code pageSize} and/or {@code pageNum} is invalid (any of them <= 0)</li>
	 * </ul>
	 * If any of the corner case happened, {@code null} shall be returned.
	 */
	@Override
	public List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum) {
		long auth_mid = VerifyAuth.verifyAuth(auth);
		if (auth_mid == -1) {
			log.error("Auth verification failed.");
			return null;
		}
		// handle invalid keywords
		if (keywords == null || keywords.isEmpty()) {
			log.error("No keyword provided.");
			return null;
		}
		// handle invalid pageSize and pageNum
		if (pageSize <= 0 || pageNum <= 0) {
			log.error("Invalid pageSize or pageNum.");
			return null;
		}
		// split keywords
		String[] keywordList = keywords.split(" +");
		String searchVideoSQL1 = """
select bv from video_info
	join (
		select bv, count(*) as cnt
			from user_watch_video group by bv
	) as watchCnt on video_info.bv = watchCnt.bv
	join user_info on ownMid = mid
	where active = true and (
		revMid is not null and publicTime <= now() -- visible to all
		or mid = ? -- visible to owner himself/herself
		or identity = 'SUPERUSER' -- visible to superuser
	)
	order by (
		(
		""";
//		array_length(regexp_matches(title, ?, 'g'), 1) +
//		array_length(regexp_matches(descr, ?, 'g'), 1) +
//		array_length(regexp_matches(name , ?, 'g'), 1) +
		String searchVideoSQL2 = """
		0) as relevance desc
		cnt desc
	) limit ? offset ?
		""";
		String searchVideoSQL = searchVideoSQL1 + ("""
			array_length(regexp_matches(title, ?, 'g'), 1) +
			array_length(regexp_matches(descr, ?, 'g'), 1) +
			array_length(regexp_matches(name , ?, 'g'), 1) +
		"""
		).repeat(keywordList.length) + searchVideoSQL2;
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(searchVideoSQL)) {
			stmt.setLong(1, auth_mid);
			for (int i = 0; i < keywordList.length; i++) {
				stmt.setString(2 + 3 * i, keywordList[i]);
				stmt.setString(3 + 3 * i, keywordList[i]);
				stmt.setString(4 + 3 * i, keywordList[i]);
			}
			stmt.setInt(2 + 3 * keywordList.length, pageSize);
			stmt.setInt(3 + 3 * keywordList.length, (pageNum - 1) * pageSize);
			try (ResultSet rs = stmt.executeQuery()) {
				List<String> result = new ArrayList<>();
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
	 *   <li>{@code bv} is invalid (null or empty or not found)</li>
	 *   <li>no one has watched this video</li>
	 * </ul>
	 * If any of the corner case happened, {@code -1} shall be returned.
	 */
	@Override
	public double getAverageViewRate(String bv) {
		// handle invalid bv
		if (bv == null || bv.isEmpty()) {
			log.error("Video not found or insufficient permission");
			return -1;
		}
		String checkBvSQL = "select count(*) from video_info where bv = ? and active = true";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(checkBvSQL)) {
			stmt.setString(1, bv);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				if (rs.getInt(1) == 0) {
					log.error("Video not found or insufficient permission");
					return -1;
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return -1;
		}
		// get average view rate
		String getAverageViewRateSQL = "select count(*), avg(lastpos) from user_watch_video where bv = ?";
		double avg;
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(getAverageViewRateSQL)) {
			stmt.setString(1, bv);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				if (rs.getInt(1) == 0) {
					log.error("No one has watched this video.");
					return -1;
				}
				avg = rs.getDouble(1);
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return -1;
		}
		// get duration
		String getDurationSQL = "select duration from video_info where bv = ?";
		float duration;
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(getDurationSQL)) {
			stmt.setString(1, bv);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				duration = rs.getFloat(1);
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return -1;
		}
		return avg / duration;
	}

	/**
	 * Gets the hotspot of a video.
	 * With splitting the video into 10-second chunks, hotspots are defined as chunks with the most danmus.
	 *
	 * @param bv the video's {@code bv}
	 * @return the index of hotspot chunks (start from 0)
	 * @apiNote You may consider the following corner cases:
	 * <ul>
	 *   <li>{@code bv} is invalid (null or empty or not found)</li>
	 *   <li>no one has sent danmu on this video</li>
	 * </ul>
	 * If any of the corner case happened, an empty set shall be returned.
	 */
	@Override
	public Set<Integer> getHotspot(String bv) {
		Set<Integer> result = new HashSet<>();
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
		// get hotspot
		String getHotspotSQL = """
with danmu_cnt as (
    select floor(showtime / 10) as chunkId, count(*) as cnt
        from danmu_info where bv = ? group by chunkId
)
select chunkId, maxx from (
    select chunkId, cnt, max(cnt) over() as maxx from danmu_cnt
) as hotspot where cnt = maxx
		""";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(getHotspotSQL)) {
			stmt.setString(1, bv);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				if (rs.getInt(2) == 0) {
					log.error("No one has sent danmu on this video.");
					return result;
				}
				result.add(rs.getInt(1));
				while (rs.next()) {
					result.add(rs.getInt(1));
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return result;
		}
		return result;
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
	 *   <li>{@code bv} is invalid (null or empty or not found)</li>
	 *   <li>{@code auth} is not a superuser or he/she is the owner</li>
	 *   <li>the video is already reviewed</li>
	 * </ul>
	 * If any of the corner case happened, {@code false} shall be returned.
	 */
	@Override
	public boolean reviewVideo(AuthInfo auth, String bv) {
		// handle invalid auth
		long auth_mid = VerifyAuth.verifyAuth(auth);
		if (auth_mid == -1) {
			log.error("Auth verification failed.");
			return false;
		}
		// handle invalid bv
		if (bv == null || bv.isEmpty()) {
			log.error("Video not found or insufficient permission");
			return false;
		}
		String checkBvSQL = "select count(*) from video_info where bv = ? and active = true";
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(checkBvSQL)) {
			stmt.setString(1, bv);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				if (rs.getInt(1) == 0) {
					log.error("Video not found or insufficient permission");
					return false;
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		// get auth identity
		String getAuthIdentitySQL = "select identity from user_info where mid = ?";
		String authIdentity;
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(getAuthIdentitySQL)) {
			stmt.setLong(1, auth_mid);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				authIdentity = rs.getString(1);
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		if (authIdentity.equals("USER")) {
			log.error("Video not found or insufficient permission");
			return false;
		}
		// get owner mid
		String getOwnerMidSQL = "select ownMid from video_info where bv = ?";
		long ownerMid;
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(getOwnerMidSQL)) {
			stmt.setString(1, bv);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				ownerMid = rs.getLong(1);
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		if (authIdentity.equals("SUPERUSER") && auth_mid == ownerMid) {
			log.error("Video not found or insufficient permission");
			return false;
		}
		// check if the video is already reviewed
		String checkReviewSQL = "select count(*) from video_info where bv = ? and revMid is not null";
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(checkReviewSQL)) {
			stmt.setString(1, bv);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				if (rs.getInt(1) > 0) {
					log.error("The video is already reviewed.");
					return false;
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		// review the video
		String reviewVideoSQL = """
update video_info
	set revMid = ?, reviewTime = ?
	where bv = ?
		""";
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(reviewVideoSQL)) {
			stmt.setLong(1, auth_mid);
			stmt.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
			stmt.setString(3, bv);
			stmt.executeUpdate();
			return true;
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
	}

	/**
	 * Donates one coin to the video. A user can at most donate one coin to a video.
	 * The user can only coin a video if he/she can search it ({@link io.sustc.service.VideoService#searchVideo(AuthInfo, String, int, int)}).
	 * It is not mandatory that the user shall watch the video first before he/she donates coin to it.
	 *
	 * @param auth the current user's authentication information
	 * @param bv   the video's {@code bv}
	 * @return whether a coin is successfully donated
	 * @apiNote You may consider the following corner cases:
	 * <ul>
	 *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
	 *   <li>{@code bv} is invalid (null or empty or not found)</li>
	 *   <li>the user cannot search this video or he/she is the owner</li>
	 *   <li>the user has no coin or has donated a coin to this video</li>
	 * </ul>
	 * If any of the corner case happened, {@code false} shall be returned.
	 */
	@Override
	public boolean coinVideo(AuthInfo auth, String bv) {
		// handle invalid auth
		long auth_mid = VerifyAuth.verifyAuth(auth);
		if (auth_mid == -1) {
			log.error("Auth verification failed.");
			return false;
		}
		// handle invalid bv
		if (bv == null || bv.isEmpty()) {
			log.error("Video not found or insufficient permission");
			return false;
		}
		String checkBvSQL = "select count(*) from video_info where bv = ? and active = true";
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(checkBvSQL)) {
			stmt.setString(1, bv);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				if (rs.getInt(1) == 0) {
					log.error("Video not found or insufficient permission");
					return false;
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		// get auth identity
		String getAuthIdentitySQL = "select identity from user_info where mid = ?";
		String authIdentity;
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(getAuthIdentitySQL)) {
			stmt.setLong(1, auth_mid);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				authIdentity = rs.getString(1);
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		long ownMid;
		String getValidBvSQL = """
select bv, ownMid from video_info
	where bv = ? and ownMid <> ? and active = true and (
		revMid is not null and publicTime <= now() -- visible to all
		or ? = 'SUPERUSER')  -- visible to super user only
		""";
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(getValidBvSQL)) {
			stmt.setString(1, bv);
			stmt.setLong(2, auth_mid);
			stmt.setString(3, authIdentity);
			try (ResultSet rs = stmt.executeQuery()) {
				if (!rs.next()) {
					log.error("Video not found or insufficient permission");
					return false;
				}
				ownMid = rs.getLong(2);
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		// check if the user has coin
		String checkCoinSQL = "select coin from user_info where mid = ?";
		int coin;
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(checkCoinSQL)) {
			stmt.setLong(1, auth_mid);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				coin = rs.getInt(1);
				if (coin == 0) {
					log.error("The user has no coin.");
					return false;
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		// check if the user has donated a coin to this video
		String checkDonationSQL = "select count(*) from user_coin_video where mid = ? and bv = ?";
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(checkDonationSQL)) {
			stmt.setLong(1, auth_mid);
			stmt.setString(2, bv);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				if (rs.getInt(1) > 0) {
					log.error("The user has donated a coin to this video.");
					return false;
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		// donate a coin to this video
		String donateCoinSQL = """
begin transaction;
	update user_info set coin = coin - 1 where mid = ?;
	update user_info set coin = coin + 1 where mid = ?;
	insert into user_coin_video (mid, bv) values (?, ?);
	do $$ begin
	    if (select coin from user_info where mid = ?) < 0 then
	        rollback;
	    elsif (select active from video_info where bv = ?) = false then
	        rollback;
	    elsif (select active from video_info where bv = ?) = false then
	        rollback;
	    else
	        commit;
	    end if;
	end $$;
		""";
		try (Connection conn = dataSource.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(donateCoinSQL)) {
			stmt.setLong(1, auth_mid);
			stmt.setLong(2, ownMid);
			stmt.setLong(3, auth_mid);
			stmt.setString(4, bv);
			stmt.setLong(5, auth_mid);
			stmt.executeUpdate();
			return true;
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
	 *   <li>{@code bv} is invalid (null or empty or not found)</li>
	 *   <li>the user cannot search this video or the user is the video owner</li>
	 * </ul>
	 * If any of the corner case happened, {@code false} shall be returned.
	 */
	@Override
	public boolean likeVideo(AuthInfo auth, String bv) {
		// handle invalid auth
		long auth_mid = VerifyAuth.verifyAuth(auth);
		if (auth_mid == -1) {
			log.error("Auth verification failed.");
			return false;
		}
		// handle invalid bv
		if (bv == null || bv.isEmpty()) {
			log.error("Video not found or insufficient permission");
			return false;
		}
		String checkBvSQL = "select count(*) from video_info where bv = ? and active = true";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(checkBvSQL)) {
			stmt.setString(1, bv);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				if (rs.getInt(1) == 0) {
					log.error("Video not found or insufficient permission");
					return false;
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		// get auth identity
		String getAuthIdentitySQL = "select identity from user_info where mid = ?";
		String authIdentity;
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(getAuthIdentitySQL)) {
			stmt.setLong(1, auth_mid);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				authIdentity = rs.getString(1);
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		String getValidBvSQL = """
select bv from video_info
	where bv = ? and ownMid <> ? and active = true and (
		revMid is not null and publicTime <= now() -- visible to all
		or ? = 'SUPERUSER')  -- visible to super user only
		""";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(getValidBvSQL)) {
			stmt.setString(1, bv);
			stmt.setLong(2, auth_mid);
			stmt.setString(3, authIdentity);
			try (ResultSet rs = stmt.executeQuery()) {
				if (!rs.next()) {
					log.error("Video not found or insufficient permission");
					return false;
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		// check if the user has liked the video
		String checkLikedSQL = "select count(*) from user_like_video where mid = ? and bv = ?";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(checkLikedSQL)) {
			stmt.setLong(1, auth_mid);
			stmt.setString(2, bv);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				if (rs.getInt(1) > 0) {
					log.error("The user has liked this video before.");
					return false;
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		// add relationship
		String InsertUserLikeSQL = "insert into user_like_video (mid, bv) values (?, ?)";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(InsertUserLikeSQL)) {
			stmt.setLong(1, auth_mid);
			stmt.setString(2, bv);
			stmt.executeUpdate();
			return true;
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
	 *   <li>{@code bv} is invalid (null or empty or not found)</li>
	 *   <li>the user cannot search this video or the user is the video owner</li>
	 * </ul>
	 * If any of the corner case happened, {@code false} shall be returned.
	 */
	@Override
	public boolean collectVideo(AuthInfo auth, String bv) {
		// handle invalid auth
		long auth_mid = VerifyAuth.verifyAuth(auth);
		if (auth_mid == -1) {
			log.error("Auth verification failed.");
			return false;
		}
		// handle invalid bv
		if (bv == null || bv.isEmpty()) {
			log.error("Video not found or insufficient permission");
			return false;
		}
		String checkBvSQL = "select count(*) from video_info where bv = ? and active = true";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(checkBvSQL)) {
			stmt.setString(1, bv);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				if (rs.getInt(1) == 0) {
					log.error("Video not found or insufficient permission");
					return false;
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		// get auth identity
		String getAuthIdentitySQL = "select identity from user_info where mid = ?";
		String authIdentity;
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(getAuthIdentitySQL)) {
			stmt.setLong(1, auth_mid);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				authIdentity = rs.getString(1);
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		String getValidBvSQL = """
select bv from video_info
	where bv = ? and ownMid <> ? and active = true and (
		revMid is not null and publicTime <= now() -- visible to all
		or ? = 'SUPERUSER') -- visible to super user only
		""";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(getValidBvSQL)) {
			stmt.setString(1, bv);
			stmt.setLong(2, auth_mid);
			stmt.setString(3, authIdentity);
			try (ResultSet rs = stmt.executeQuery()) {
				if (!rs.next()) {
					log.error("Video not found or insufficient permission");
					return false;
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		// check if the user has liked the video
		String checkFavedSQL = "select count(*) from user_fav_video where mid = ? and bv = ?";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(checkFavedSQL)) {
			stmt.setLong(1, auth_mid);
			stmt.setString(2, bv);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				if (rs.getInt(1) > 0) {
					log.error("The user has collected this video before.");
					return false;
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		// add relationship
		String InsertUserFavSQL = "insert into user_fav_video (mid, bv) values (?, ?)";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(InsertUserFavSQL)) {
			stmt.setLong(1, auth_mid);
			stmt.setString(2, bv);
			stmt.executeUpdate();
			return true;
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
	}
}
