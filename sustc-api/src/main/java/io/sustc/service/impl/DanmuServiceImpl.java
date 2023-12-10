package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.DanmuService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class DanmuServiceImpl implements DanmuService {
	@Autowired
	private DataSource dataSource;


	/**
	 * Sends a danmu to a video.
	 * It is mandatory that the user shall watch the video first before he/she can send danmu to it.
	 *
	 * @param auth    the current user's authentication information
	 * @param bv      the video's bv
	 * @param content the content of danmu
	 * @param time    seconds since the video starts
	 * @return the generated danmu id
	 * @apiNote You may consider the following corner cases:
	 * <ul>
	 *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
	 *   <li>cannot find a video corresponding to the {@code bv}</li>
	 *   <li>{@code content} is invalid (null or empty)</li>
	 *   <li>the video is not published or the user has not watched this video</li>
	 * </ul>
	 * If any of the corner case happened, {@code -1} shall be returned.
	 */
	@Override
	public long sendDanmu(AuthInfo auth, String bv, String content, float time) {
		// handle invalid auth
		long auth_mid = VerifyAuth.verifyAuth(auth);
		if (auth_mid == -1) {
			log.error("Auth verification failed.");
			return -1;
		}
		// handle invalid bv and status
		if (bv == null || bv.isEmpty()) {
			log.error("Video not found or insufficient permission");
			return -1;
		}
		String checkBvSQL = """
select count(*) from video_info where bv = ? and active = true
	and revMid is not null and (publicTime is null or publicTime < now())
		""";
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
		// the user hasn't watched the video
		String checkWatchedSQL = """
select count(*) from user_watch_video
	where mid = ? and bv = ?
		""";
		try (Connection conn = dataSource.getConnection();
		         PreparedStatement stmt = conn.prepareStatement(checkWatchedSQL)) {
			stmt.setLong(1, auth_mid);
			stmt.setString(2, bv);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				if (rs.getInt(1) == 0) {
					log.error("Please comment after you've watched the video.");
					return -1;
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return -1;
		}
		// add danmu
		String addDanmuSQL = """
insert into danmu_info (bv, senderMid, showTime, content, postTime) values (?, ?, ?, ?, ?);
select danmu_id from danmu_info where bv = ? and senderMid = ? and postTime = ?
		""";try (Connection conn = dataSource.getConnection();
		         PreparedStatement stmt = conn.prepareStatement(addDanmuSQL)) {
			stmt.setString(1, bv);
			stmt.setLong(2, auth_mid);
			stmt.setFloat(3, time);
			stmt.setString(4, content);
			Timestamp ts = Timestamp.valueOf(LocalDateTime.now());
			stmt.setTimestamp(5, ts);
			stmt.setString(6, bv);
			stmt.setLong(7, auth_mid);
			stmt.setTimestamp(8, ts);
			try(ResultSet rs = stmt.executeQuery()) {
				rs.next();
				return rs.getInt(1);
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return -1;
		}
	}

	/**
	 * Display the danmus in a time range.
	 * Similar to bilibili's mechanism, user can choose to only display part of the danmus to have a better watching
	 * experience.
	 *
	 * @param bv        the video's bv
	 * @param timeStart the start time of the range
	 * @param timeEnd   the end time of the range
	 * @param filter    whether to remove the duplicated content,
	 *                  if {@code true}, only the earliest posted danmu with the same content shall be returned
	 * @return a list of danmus id, sorted by {@code time}
	 * @apiNote You may consider the following corner cases:
	 * <ul>
	 *   <li>{@code bv} is invalid (null or empty or not found)</li>
	 *   <li>
	 *     {@code timeStart} and/or {@code timeEnd} is invalid ({@code timeStart} <= {@code timeEnd}
	 *     or any of them < 0 or > video duration)
	 *   </li>
	 * <li>the video is not published</li>
	 * </ul>
	 * If any of the corner case happened, {@code null} shall be returned.
	 */
	@Override
	public List<Long> displayDanmu(String bv, float timeStart, float timeEnd, boolean filter) {
		List<Long> result = new ArrayList<>();
		// handle invalid bv and status
		if (bv == null || bv.isEmpty()) {
			log.error("Video not found or insufficient permission");
			return result;
		}
		String checkBvSQL = """
select duration from video_info where bv = ? and active = true
	and revMid is not null and (publicTime is null or publicTime < now())
		""";
		float duration;
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(checkBvSQL)) {
			stmt.setString(1, bv);
			try (ResultSet rs = stmt.executeQuery()) {
				if (!rs.next()) {
					log.error("Video not found or insufficient permission");
					return result;
				}
				duration = rs.getFloat(1);
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return result;
		}
		// handle invalid time interval
		if (timeStart > timeEnd || timeStart < 0 || timeEnd > duration) {
			log.error("Invalid interval.");
			return result;
		}
		// retrieve danmu in the specified time interval
		String selectDanmuSQL;
		if (filter) {
			selectDanmuSQL = """
with allDanmu as (
	select danmu_id, content, postTime from danmu_info
		where bv = ? and active = true
		and showTime between ? and ?
)
select danmu_id from (
	select danmu_id, postTime,
		min(posttime) over(partition by content) as firstPosted
	from allDanmu
) as DPP where postTime = firstPosted;
			""";
		}
		else {
			selectDanmuSQL = """
select danmu_id from danmu_info
	where bv = ? and active = true
	and showTime between ? and ?
			""";
		}
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(selectDanmuSQL)) {
			stmt.setString(1, bv);
			stmt.setFloat(2, timeStart);
			stmt.setFloat(3, timeEnd);
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					result.add(rs.getLong(1));
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return result;
		}
		return result;
	}

	/**
	 * Likes a danmu.
	 * If the user already liked the danmu, this operation will cancel the like status.
	 * It is mandatory that the user shall watch the video first before he/she can like a danmu of it.
	 *
	 * @param auth the current user's authentication information
	 * @param id   the danmu's id
	 * @return the like state of the user to this danmu after this operation
	 * @apiNote You may consider the following corner cases:
	 * <ul>
	 *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
	 *   <li>{@code id} is invalid (<= 0 or not found)</li>
	 * </ul>
	 * If any of the corner case happened, {@code false} shall be returned.
	 */
	@Override
	public boolean likeDanmu(AuthInfo auth, long id) {
		// handle invalid auth
		long auth_mid = VerifyAuth.verifyAuth(auth);
		if (auth_mid == -1) {
			log.error("Auth verification failed.");
			return false;
		}
		// handle invalid id
		String checkDanmuSQL = """
select count(*) from danmu_info
	where danmu_id = ? and active = true
		""";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(checkDanmuSQL)) {
			stmt.setLong(1, id);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				if (rs.getInt(1) == 0) {
					log.error("Danmu not found.");
					return false;
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		// check if the user has liked this danmu
		String DupCheckSQL = """
select count(*) from user_like_danmu
	where danmu_id = ? and mid = ?
		""";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(DupCheckSQL)) {
			stmt.setLong(1, id);
			stmt.setLong(1, auth_mid);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				if (rs.getInt(1) > 0) {
					log.error("You've liked this danmu before");
					return false;
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		// insert like into SQL
		String insertLikeSQL = """
insert into user_like_danmu (danmu_id, mid) values (?, ?)
		""";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(insertLikeSQL)) {
			stmt.setLong(1, id);
			stmt.setLong(1, auth_mid);
			stmt.executeQuery();
			return true;
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
	}
}
