package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.DanmuService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
		if (content == null || content.isEmpty()) {
			return -1;
		}
		String sendDanmuSQL = "select send_danmu(?, ?, ?, ?, ?, ?, ?)";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(sendDanmuSQL)) {
			stmt.setLong(1, auth.getMid());
			stmt.setString(2, auth.getPassword());
			stmt.setString(3, auth.getQq());
			stmt.setString(4, auth.getWechat());
			stmt.setString(5, bv);
			stmt.setString(6, content);
			stmt.setFloat(7, time);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				return rs.getLong(1);
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
	 *   <li>cannot find a video corresponding to the {@code bv}</li>
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
		if (timeStart > timeEnd) {
			return Collections.emptyList();
		}
		String displayDanmuSQL = "select display_danmu(?, ?, ?, ?)";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(displayDanmuSQL)) {
			stmt.setString(1, bv);
			stmt.setFloat(2, timeStart);
			stmt.setFloat(3, timeEnd);
			stmt.setBoolean(4, filter);
			try (ResultSet rs = stmt.executeQuery()) {
				if (rs.next()) {
					if (rs.getArray(1) == null) {
						return Collections.emptyList();
					}
					return new ArrayList<>(Arrays.asList((Long[]) rs.getArray(1).getArray()));
				}
				else {
					return Collections.emptyList();
				}
			}
		} catch (SQLException e) {
//			log.error("SQL error: {}", e.getMessage());
			return Collections.emptyList();
		}
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
	 *   <li>cannot find a danmu corresponding to the {@code id}</li>
	 * </ul>
	 * If any of the corner case happened, {@code false} shall be returned.
	 */
	@Override
	public boolean likeDanmu(AuthInfo auth, long id) {
		String likeDanmuSQL = "select like_danmu(?, ?, ?, ?, ?)";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(likeDanmuSQL)) {
			stmt.setLong(1, auth.getMid());
			stmt.setString(2, auth.getPassword());
			stmt.setString(3, auth.getQq());
			stmt.setString(4, auth.getWechat());
			stmt.setLong(5, id);
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
