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
		// handle invalid author
		long auth_mid = VerifyAuth.verifyAuth(auth);
		if (auth_mid == -1) {
			return null;
		}
		// handle invalid title
		if (req.getTitle() == null || req.getTitle().isEmpty()) {
			log.error("Video title is empty.");
			return null;
		}
		// handle invalid duration
		if (req.getDuration() < 10) {
			log.error("The video is too short (less than 10 seconds).");
			return null;
		}
		// handle invalid public time
		if (req.getPublicTime() != null && req.getPublicTime().toLocalDateTime().isBefore(LocalDateTime.now())) {
			log.error("Public time is earlier than current time.");
			return null;
		}
		// handle duplicate video
		String dupCheckSQL = "select count(*) from video_info where title = ? and mid = ? and active = true";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(dupCheckSQL)) {
			stmt.setString(1, req.getTitle());
			stmt.setLong(2, auth_mid);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				if (rs.getInt(1) > 0) {
					log.error("Title has been used.");
					return null;
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return null;
		}
		// assign a bv to the video
	}

	@Override
	public boolean deleteVideo(AuthInfo auth, String bv) {
		return false;
	}

	@Override
	public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req) {
		return false;
	}

	@Override
	public List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum) {
		return null;
	}

	@Override
	public double getAverageViewRate(String bv) {
		return 0;
	}

	@Override
	public Set<Integer> getHotspot(String bv) {
		return null;
	}

	@Override
	public boolean reviewVideo(AuthInfo auth, String bv) {
		return false;
	}

	@Override
	public boolean coinVideo(AuthInfo auth, String bv) {
		return false;
	}

	@Override
	public boolean likeVideo(AuthInfo auth, String bv) {
		return false;
	}

	@Override
	public boolean collectVideo(AuthInfo auth, String bv) {
		return false;
	}
}
