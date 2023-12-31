package io.sustc.service.impl;

import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {
	static boolean startModify = false;
	static int userFollowChunkSize;

	/**
	 * Getting a {@link DataSource} instance from the framework, whose connections are managed by HikariCP.
	 * <p>
	 * Marking a field with {@link Autowired} annotation enables our framework to automatically
	 * provide you a well-configured instance of {@link DataSource}.
	 * Learn more: <a href="https://www.baeldung.com/spring-dependency-injection">Dependency Injection</a>
	 */
	@Autowired
	private DataSource dataSource;

	/**
	 * Acknowledges the authors of this project.
	 *
	 * @return a list of group members' student-id
	 */
	@Override
	public List<Integer> getGroupMembers() {
		return List.of(12212232);
	}


	class ImportThread1 extends Thread {
		List<UserRecord> userRecords;

		public ImportThread1(List<UserRecord> userRecords) {
			this.userRecords = userRecords;
		}

		@Override
		public void run() {
			// load user_follow
			String insertUserFollowSQL = "insert into user_follow values (?, ?);";
			try (Connection conn = dataSource.getConnection();
			     PreparedStatement stmt = conn.prepareStatement(insertUserFollowSQL)) {
				conn.setAutoCommit(false);
				for (int i = 0; i < userFollowChunkSize; ++i) {
					UserRecord userRecord = userRecords.get(i);
					stmt.setLong(2, userRecord.getMid());
					for (Long starMid : userRecord.getFollowing()) {
						stmt.setLong(1, starMid);
						stmt.addBatch();
					}
				}
				stmt.executeBatch();
				conn.commit();
				conn.setAutoCommit(true);
			} catch (SQLException e) {
				System.out.println("[ERROR] Fail to insert user_follow records, " + e.getMessage());
			}
		}
	}

	class ImportThread2 extends Thread {
		List<UserRecord> userRecords;

		public ImportThread2(List<UserRecord> userRecords) {
			this.userRecords = userRecords;
		}

		@Override
		public void run() {
			// load user_follow
			String insertUserFollowSQL = "insert into user_follow values (?, ?);";
			try (Connection conn = dataSource.getConnection();
			     PreparedStatement stmt = conn.prepareStatement(insertUserFollowSQL)) {
				conn.setAutoCommit(false);
				for (int i = userFollowChunkSize; i < userFollowChunkSize * 2; ++i) {
					UserRecord userRecord = userRecords.get(i);
					stmt.setLong(2, userRecord.getMid());
					for (Long starMid : userRecord.getFollowing()) {
						stmt.setLong(1, starMid);
						stmt.addBatch();
					}
				}
				stmt.executeBatch();
				conn.commit();
				conn.setAutoCommit(true);
			} catch (SQLException e) {
				System.out.println("[ERROR] Fail to insert user_follow records, " + e.getMessage());
			}
		}
	}


	class ImportThread3 extends Thread {
		List<UserRecord> userRecords;

		public ImportThread3(List<UserRecord> userRecords) {
			this.userRecords = userRecords;
		}

		@Override
		public void run() {
			// load user_follow
			String insertUserFollowSQL = "insert into user_follow values (?, ?);";
			try (Connection conn = dataSource.getConnection();
			     PreparedStatement stmt = conn.prepareStatement(insertUserFollowSQL)) {
				conn.setAutoCommit(false);
				for (int i = userFollowChunkSize * 2; i < userFollowChunkSize * 3; ++i) {
					UserRecord userRecord = userRecords.get(i);
					stmt.setLong(2, userRecord.getMid());
					for (Long starMid : userRecord.getFollowing()) {
						stmt.setLong(1, starMid);
						stmt.addBatch();
					}
				}
				stmt.executeBatch();
				conn.commit();
				conn.setAutoCommit(true);
			} catch (SQLException e) {
				System.out.println("[ERROR] Fail to insert user_follow records, " + e.getMessage());
			}
		}
	}


	class ImportThread4 extends Thread {
		List<UserRecord> userRecords;

		public ImportThread4(List<UserRecord> userRecords) {
			this.userRecords = userRecords;
		}

		@Override
		public void run() {
			// load user_follow
			String insertUserFollowSQL = "insert into user_follow values (?, ?);";
			try (Connection conn = dataSource.getConnection();
			     PreparedStatement stmt = conn.prepareStatement(insertUserFollowSQL)) {
				conn.setAutoCommit(false);
				for (int i = userFollowChunkSize * 3; i < userFollowChunkSize * 4; ++i) {
					UserRecord userRecord = userRecords.get(i);
					stmt.setLong(2, userRecord.getMid());
					for (Long starMid : userRecord.getFollowing()) {
						stmt.setLong(1, starMid);
						stmt.addBatch();
					}
				}
				stmt.executeBatch();
				conn.commit();
				conn.setAutoCommit(true);
			} catch (SQLException e) {
				System.out.println("[ERROR] Fail to insert user_follow records, " + e.getMessage());
			}
		}
	}


	class ImportThread5 extends Thread {
		List<UserRecord> userRecords;

		public ImportThread5(List<UserRecord> userRecords) {
			this.userRecords = userRecords;
		}

		@Override
		public void run() {
			// load user_follow
			String insertUserFollowSQL = "insert into user_follow values (?, ?);";
			try (Connection conn = dataSource.getConnection();
			     PreparedStatement stmt = conn.prepareStatement(insertUserFollowSQL)) {
				conn.setAutoCommit(false);
				for (int i = userFollowChunkSize * 4; i < userFollowChunkSize * 5; ++i) {
					UserRecord userRecord = userRecords.get(i);
					stmt.setLong(2, userRecord.getMid());
					for (Long starMid : userRecord.getFollowing()) {
						stmt.setLong(1, starMid);
						stmt.addBatch();
					}
				}
				stmt.executeBatch();
				conn.commit();
				conn.setAutoCommit(true);
			} catch (SQLException e) {
				System.out.println("[ERROR] Fail to insert user_follow records, " + e.getMessage());
			}
		}
	}


	class ImportThread6 extends Thread {
		List<UserRecord> userRecords;

		public ImportThread6(List<UserRecord> userRecords) {
			this.userRecords = userRecords;
		}

		@Override
		public void run() {
			// load user_follow
			String insertUserFollowSQL = "insert into user_follow values (?, ?);";
			try (Connection conn = dataSource.getConnection();
			     PreparedStatement stmt = conn.prepareStatement(insertUserFollowSQL)) {
				conn.setAutoCommit(false);
				for (int i = userFollowChunkSize * 5; i < userFollowChunkSize * 6; ++i) {
					UserRecord userRecord = userRecords.get(i);
					stmt.setLong(2, userRecord.getMid());
					for (Long starMid : userRecord.getFollowing()) {
						stmt.setLong(1, starMid);
						stmt.addBatch();
					}
				}
				stmt.executeBatch();
				conn.commit();
				conn.setAutoCommit(true);
			} catch (SQLException e) {
				System.out.println("[ERROR] Fail to insert user_follow records, " + e.getMessage());
			}
		}
	}


	class ImportThread7 extends Thread {
		List<UserRecord> userRecords;

		public ImportThread7(List<UserRecord> userRecords) {
			this.userRecords = userRecords;
		}

		@Override
		public void run() {
			// load user_follow
			String insertUserFollowSQL = "insert into user_follow values (?, ?);";
			try (Connection conn = dataSource.getConnection();
			     PreparedStatement stmt = conn.prepareStatement(insertUserFollowSQL)) {
				conn.setAutoCommit(false);
				for (int i = userFollowChunkSize * 6; i < userFollowChunkSize * 7; ++i) {
					UserRecord userRecord = userRecords.get(i);
					stmt.setLong(2, userRecord.getMid());
					for (Long starMid : userRecord.getFollowing()) {
						stmt.setLong(1, starMid);
						stmt.addBatch();
					}
				}
				stmt.executeBatch();
				conn.commit();
				conn.setAutoCommit(true);
			} catch (SQLException e) {
				System.out.println("[ERROR] Fail to insert user_follow records, " + e.getMessage());
			}
		}
	}


	class ImportThread8 extends Thread {
		List<UserRecord> userRecords;

		public ImportThread8(List<UserRecord> userRecords) {
			this.userRecords = userRecords;
		}

		@Override
		public void run() {
			// load user_follow
			String insertUserFollowSQL = "insert into user_follow values (?, ?);";
			try (Connection conn = dataSource.getConnection();
			     PreparedStatement stmt = conn.prepareStatement(insertUserFollowSQL)) {
				conn.setAutoCommit(false);
				for (int i = userFollowChunkSize * 7; i < userFollowChunkSize * 8; ++i) {
					UserRecord userRecord = userRecords.get(i);
					stmt.setLong(2, userRecord.getMid());
					for (Long starMid : userRecord.getFollowing()) {
						stmt.setLong(1, starMid);
						stmt.addBatch();
					}
				}
				stmt.executeBatch();
				conn.commit();
				conn.setAutoCommit(true);
			} catch (SQLException e) {
				System.out.println("[ERROR] Fail to insert user_follow records, " + e.getMessage());
			}
		}
	}

	class ImportThread9 extends Thread {
		List<UserRecord> userRecords;

		public ImportThread9(List<UserRecord> userRecords) {
			this.userRecords = userRecords;
		}

		@Override
		public void run() {
			// load user_follow
			String insertUserFollowSQL = "insert into user_follow values (?, ?);";
			try (Connection conn = dataSource.getConnection();
			     PreparedStatement stmt = conn.prepareStatement(insertUserFollowSQL)) {
				conn.setAutoCommit(false);
				for (int i = userFollowChunkSize * 8; i < userRecords.size(); ++i) {
					UserRecord userRecord = userRecords.get(i);
					stmt.setLong(2, userRecord.getMid());
					for (Long starMid : userRecord.getFollowing()) {
						stmt.setLong(1, starMid);
						stmt.addBatch();
					}
				}
				stmt.executeBatch();
				conn.commit();
				conn.setAutoCommit(true);
			} catch (SQLException e) {
				System.out.println("[ERROR] Fail to insert user_follow records, " + e.getMessage());
			}
		}
	}

	class ImportThread10 extends Thread {
		private final List<VideoRecord> videoRecords;

		public ImportThread10(List<VideoRecord> videoRecords) {
			this.videoRecords = videoRecords;
		}

		public void run() {
			// load user_coin_video
			String insertUserCoinVideoSQL = "insert into user_coin_video values (?, ?);";
			try (Connection conn = dataSource.getConnection();
			     PreparedStatement stmt = conn.prepareStatement(insertUserCoinVideoSQL)) {
				conn.setAutoCommit(false);
				for (VideoRecord videoRecord : videoRecords) {
					stmt.setString(2, videoRecord.getBv());
					for (Long mid : videoRecord.getCoin()) {
						stmt.setLong(1, mid);
						stmt.addBatch();
					}
				}
				stmt.executeBatch();
				conn.commit();
				conn.setAutoCommit(true);
			} catch (SQLException e) {
				System.out.println("[ERROR] Fail to insert user_coin_video records, " + e.getMessage());
			}

			// load user_fav_video
			String insertUserFavVideoSQL = "insert into user_fav_video values (?, ?);";
			try (Connection conn = dataSource.getConnection();
			     PreparedStatement stmt = conn.prepareStatement(insertUserFavVideoSQL)) {
				conn.setAutoCommit(false);
				for (VideoRecord videoRecord : videoRecords) {
					stmt.setString(2, videoRecord.getBv());
					for (Long mid : videoRecord.getFavorite()) {
						stmt.setLong(1, mid);
						stmt.addBatch();
					}
				}
				stmt.executeBatch();
				conn.commit();
				conn.setAutoCommit(true);
			} catch (SQLException e) {
				System.out.println("[ERROR] Fail to insert user_fav_video records, " + e.getMessage());
			}
		}
	}

	class ImportThread11 extends Thread {
		private final List<UserRecord> userRecords;
		private final List<VideoRecord> videoRecords;
		private final List<DanmuRecord> danmuRecords;

		public ImportThread11(List<UserRecord> userRecords, List<VideoRecord> videoRecords, List<DanmuRecord> danmuRecords) {
			this.userRecords = userRecords;
			this.videoRecords = videoRecords;
			this.danmuRecords = danmuRecords;
		}

		public void run() {
			// load user_info
			String insertUserInfoSQL = "insert into user_info values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, true);";
			// digest(?, 'sha256')
			try (Connection conn = dataSource.getConnection();
			     PreparedStatement stmt = conn.prepareStatement(insertUserInfoSQL)) {
				conn.setAutoCommit(false);
				for (UserRecord userRecord : userRecords) {
					stmt.setLong(1, userRecord.getMid());
					stmt.setString(2, userRecord.getName());
					stmt.setString(3, userRecord.getSex());
					stmt.setString(4, userRecord.getBirthday());
					stmt.setShort(5, userRecord.getLevel());
					stmt.setString(6, userRecord.getSign());
					stmt.setString(7, userRecord.getIdentity().name().equals("USER") ? "USER" : "SUPER");
					stmt.setString(8, userRecord.getPassword());
					stmt.setString(9, userRecord.getQq());
					stmt.setString(10, userRecord.getWechat());
					stmt.setInt(11, userRecord.getCoin());
					stmt.addBatch();
				}
				stmt.executeBatch();
				conn.commit();
				conn.setAutoCommit(true);
			} catch (SQLException e) {
				System.out.println("[ERROR] Fail to insert user records, " + e.getMessage());
			}

			// load video_info
			String insertVideoInfoSQL = """
insert into video_info (bv, title, ownMid, commitTime, revMid, reviewTime, publicTime, duration, descr)
	values (?, ?, ?, ?, ?, ?, ?, ?, ?);
			""";
			try (Connection conn = dataSource.getConnection();
			     PreparedStatement stmt = conn.prepareStatement(insertVideoInfoSQL)) {
				conn.setAutoCommit(false);
				for (VideoRecord videoRecord : videoRecords) {
					stmt.setString(1, videoRecord.getBv());
					stmt.setString(2, videoRecord.getTitle());
					stmt.setLong(3, videoRecord.getOwnerMid());
					stmt.setTimestamp(4, videoRecord.getCommitTime());
					stmt.setLong(5, videoRecord.getReviewer());
					stmt.setTimestamp(6, videoRecord.getReviewTime());
					stmt.setTimestamp(7, videoRecord.getPublicTime());
					stmt.setFloat(8, videoRecord.getDuration());
					stmt.setString(9, videoRecord.getDescription());
					stmt.addBatch();
				}
				stmt.executeBatch();
				conn.commit();
				conn.setAutoCommit(true);
			} catch (SQLException e) {
				System.out.println("[ERROR] Fail to insert video records, " + e.getMessage());
			}

			// load danmu_info
			String insertDanmuInfoSQL = "insert into danmu_info values (?, ?, ?, ?, ?, ?, true);";
			try (Connection conn = dataSource.getConnection();
			     PreparedStatement stmt = conn.prepareStatement(insertDanmuInfoSQL)) {
				conn.setAutoCommit(false);
				long danmuCnt = 0;
				for (DanmuRecord danmuRecord : danmuRecords) {
					danmuRecord.setDanmuId(++danmuCnt);
					stmt.setLong(1, danmuCnt);
					stmt.setString(2, danmuRecord.getBv());
					stmt.setLong(3, danmuRecord.getMid());
					stmt.setFloat(4, danmuRecord.getTime());
					stmt.setString(5, danmuRecord.getContent());
					stmt.setTimestamp(6, danmuRecord.getPostTime());
					stmt.addBatch();
				}
				stmt.executeBatch();
				conn.commit();
				conn.setAutoCommit(true);
			} catch (SQLException e) {
				System.out.println("[ERROR] Fail to insert danmu records, " + e.getMessage());
			}

			// load user_like_video
			String insertUserLikeVideoSQL = "insert into user_like_video values (?, ?);";
			try (Connection conn = dataSource.getConnection();
			     PreparedStatement stmt = conn.prepareStatement(insertUserLikeVideoSQL)) {
				conn.setAutoCommit(false);
				for (VideoRecord videoRecord : videoRecords) {
					stmt.setString(2, videoRecord.getBv());
					for (Long mid : videoRecord.getLike()) {
						stmt.setLong(1, mid);
						stmt.addBatch();
					}
				}
				stmt.executeBatch();
				conn.commit();
				conn.setAutoCommit(true);
			} catch (SQLException e) {
				System.out.println("[ERROR] Fail to insert user_like_video records, " + e.getMessage());
			}

			// load user_like_danmu
			String insertUserLikeDanmuSQL = "insert into user_like_danmu values (?, ?);";
			try (Connection conn = dataSource.getConnection();
			     PreparedStatement stmt = conn.prepareStatement(insertUserLikeDanmuSQL)) {
				conn.setAutoCommit(false);
				for (DanmuRecord danmuRecord : danmuRecords) {
					stmt.setLong(1, danmuRecord.getDanmuId());
					for (Long mid : danmuRecord.getLikedBy()) {
						stmt.setLong(2, mid);
						stmt.addBatch();
					}
				}
				stmt.executeBatch();
				conn.commit();
				conn.setAutoCommit(true);
			} catch (SQLException e) {
				System.out.println("[ERROR] Fail to insert user_like_danmu records, " + e.getMessage());
			}
		}
	}

	class ImportThread12 extends Thread {
		private final List<VideoRecord> videoRecords;

		public ImportThread12(List<VideoRecord> videoRecords) {
			this.videoRecords = videoRecords;
		}

		public void run() {
			// load user_watch_video
			String insertUserWatchVideoSQL = "insert into user_watch_video values (?, ?, ?);";
			try (Connection conn = dataSource.getConnection();
			     PreparedStatement stmt = conn.prepareStatement(insertUserWatchVideoSQL)) {
				conn.setAutoCommit(false);
				for (VideoRecord videoRecord : videoRecords) {
					stmt.setString(2, videoRecord.getBv());
					int viewerCnt = videoRecord.getViewerMids().length;
					for (int i = 0; i < viewerCnt; i++) {
						stmt.setLong(1, videoRecord.getViewerMids()[i]);
						stmt.setFloat(3, videoRecord.getViewTime()[i]);
						stmt.addBatch();
					}
				}
				stmt.executeBatch();
				conn.commit();
				conn.setAutoCommit(true);
			} catch (SQLException e) {
				System.out.println("[ERROR] Fail to insert user_watch_video records, " + e.getMessage());
			}
		}
	}


	/**
	 * Imports data to an empty database.
	 * Invalid data will not be provided.
	 *
	 * @param danmuRecords danmu records parsed from csv
	 * @param userRecords  user records parsed from csv
	 * @param videoRecords video records parsed from csv
	 */
	@Override
	public void importData(
		List<DanmuRecord> danmuRecords,
		List<UserRecord> userRecords,
		List<VideoRecord> videoRecords
	) {
		String createSQL = """
-- drop all tables
drop table if exists user_like_danmu;
drop table if exists user_fav_video;
drop table if exists user_like_video;
drop table if exists user_coin_video;
drop table if exists user_watch_video;
drop table if exists user_follow;
drop view if exists danmu_active;
drop view if exists video_active_super;
drop view if exists video_active;
drop view if exists user_active;
drop table if exists danmu_info;
drop table if exists video_info;
drop table if exists user_info;

-- create tables
create table user_info (
    mid bigserial not null,
    name text not null,
    sex varchar(10),
    birthday varchar(10),
    level smallint not null default 0,
    sign text,
    identity varchar(5) not null default 'USER',
    pwd char(256), -- encrypted by SHA256
    qqid varchar(50),
    wxid varchar(50),
    coin int default 0,
    active boolean default true
);

create table video_info (
    bv varchar(25) not null,
    title text not null,
    ownMid bigint not null, -- owner's mid
    commitTime timestamp,
    revMid bigint, -- reviewer's mid
    reviewTime timestamp,
    publicTime timestamp,
    duration float8, -- in seconds
    descr text, -- description
    active boolean default true
        -- only means not deleted, may not be visible
);

create table danmu_info (
    danmu_id bigserial not null,
    bv varchar(25) not null,
    senderMid bigint not null,
    showTime float8 not null,
        -- the display time from the start of video (in seconds)
    content text,
    postTime timestamp,
    active boolean default true
);

create table user_follow (
    star_mid bigint not null,
    fan_mid bigint not null
);

create table user_watch_video (
    mid bigint not null,
    bv varchar(25) not null,
    lastpos float8 not null -- last watch time stamp in seconds
);

create table user_coin_video (
    mid bigint not null,
    bv varchar(25) not null
);

create table user_like_video (
    mid bigint not null,
    bv varchar(25) not null
);

create table user_fav_video (
    mid bigint not null,
    bv varchar(25) not null
);

create table user_like_danmu (
    danmu_id bigint not null,
    mid bigint not null
);
		""";
		try (Connection conn = dataSource.getConnection();
			PreparedStatement stmt = conn.prepareStatement(createSQL)) {
			stmt.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

		userFollowChunkSize = userRecords.size() / 9;
		ImportThread1 importThread1 = new ImportThread1(userRecords);
		ImportThread2 importThread2 = new ImportThread2(userRecords);
		ImportThread3 importThread3 = new ImportThread3(userRecords);
		ImportThread4 importThread4 = new ImportThread4(userRecords);
		ImportThread5 importThread5 = new ImportThread5(userRecords);
		ImportThread6 importThread6 = new ImportThread6(userRecords);
		ImportThread7 importThread7 = new ImportThread7(userRecords);
		ImportThread8 importThread8 = new ImportThread8(userRecords);
		ImportThread9 importThread9 = new ImportThread9(userRecords);
		ImportThread10 importThread10 = new ImportThread10(videoRecords);
		ImportThread11 importThread11 = new ImportThread11(userRecords, videoRecords, danmuRecords);
		ImportThread12 importThread12 = new ImportThread12(videoRecords);
		importThread1.start();
		importThread2.start();
		importThread3.start();
		importThread4.start();
		importThread5.start();
		importThread6.start();
		importThread7.start();
		importThread8.start();
		importThread9.start();
		importThread10.start();
		importThread11.start();
		importThread12.start();
		try {
			importThread1.join();
			importThread2.join();
			importThread3.join();
			importThread4.join();
			importThread5.join();
			importThread6.join();
			importThread7.join();
			importThread8.join();
			importThread9.join();
			importThread10.join();
			importThread11.join();
			importThread12.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}


		String createFunctions = """
-- add keys and constraints
alter table user_info add constraint mid_pk primary key (mid);
-- alter table user_info add constraint sex_valid check (sex in ('MALE', 'FEMALE', 'UNKNOWN'));
alter table user_info add constraint identity_valid check (identity in ('USER', 'SUPER'));
alter table user_info add constraint coin_non_neg check (coin >= 0);
alter table user_info add constraint level_valid check (level between 0 and 6);

alter table video_info add constraint bv_pk primary key (bv);

alter table danmu_info add constraint danmu_id_pk primary key (danmu_id);
-- alter table danmu_info add constraint mid_fk foreign key (senderMid) references user_info(mid);
-- alter table danmu_info add constraint bv_fk foreign key (bv) references video_info(bv);

alter table user_follow add constraint user_follow_pk primary key (star_mid, fan_mid);
-- alter table user_follow add constraint star_fk foreign key (star_mid) references user_info(mid);
-- alter table user_follow add constraint fan_fk foreign key (fan_mid) references user_info(mid);

alter table user_watch_video add constraint user_watch_video_pk primary key (mid, bv);
-- alter table user_watch_video add constraint mid_fk foreign key (mid) references user_info(mid);
-- alter table user_watch_video add constraint bv_fk foreign key (bv) references video_info(bv);

alter table user_coin_video add constraint user_coin_video_pk primary key (mid, bv);
-- alter table user_coin_video add constraint mid_fk foreign key (mid) references user_info(mid);
-- alter table user_coin_video add constraint bv_fk foreign key (bv) references video_info(bv);

alter table user_like_video add constraint user_like_video_pk primary key (mid, bv);
-- alter table user_like_video add constraint mid_fk foreign key (mid) references user_info(mid);
-- alter table user_like_video add constraint bv_fk foreign key (bv) references video_info(bv);

alter table user_fav_video add constraint user_fav_video_pk primary key (mid, bv);
-- alter table user_fav_video add constraint mid_fk foreign key (mid) references user_info(mid);
-- alter table user_fav_video add constraint bv_fk foreign key (bv) references video_info(bv);

alter table user_like_danmu add constraint user_like_danmu_pk primary key (danmu_id, mid);
-- alter table user_like_danmu add constraint mid_fk foreign key (mid) references user_info(mid);
-- alter table user_like_danmu add constraint did_fk foreign key (danmu_id) references danmu_info(danmu_id);

alter sequence user_info_mid_seq restart with 10000000;
alter sequence danmu_info_danmu_id_seq restart with 10000000;

-- create index
create index user_info_name_idx on user_info (name) where active = true;
create index user_info_pwd_idx on user_info (pwd) where active = true;
create index user_info_qqid_idx on user_info (qqid) where active = true;
create index user_info_wxid_idx on user_info (wxid) where active = true;
create index video_info_title_idx on video_info (title) where active = true;
create index video_info_ownMid_idx on video_info (ownMid) where active = true;
create index danmu_info_bv_idx on danmu_info (bv) where active = true;
create index danmu_info_senderMid_idx on danmu_info (senderMid) where active = true;
create index danmu_info_showTime_idx on danmu_info (showTime) where active = true;
create index user_follow_star_mid_idx on user_follow (star_mid);
create index user_follow_fan_mid_idx on user_follow (fan_mid);
create index user_watch_video_mid_idx on user_watch_video (mid);
create index user_watch_video_bv_idx on user_watch_video (bv);
create index user_coin_video_mid_idx on user_coin_video (mid);
create index user_coin_video_bv_idx on user_coin_video (bv);
create index user_like_video_mid_idx on user_like_video (mid);
create index user_like_video_bv_idx on user_like_video (bv);
create index user_fav_video_mid_idx on user_fav_video (mid);
create index user_fav_video_bv_idx on user_fav_video (bv);
create index user_like_danmu_mid_idx on user_like_danmu (mid);
create index user_like_danmu_danmu_id_idx on user_like_danmu (danmu_id);


-- create views
create or replace view user_active as
	select * from user_info where active = true;

create or replace view video_active as
	select * from video_info where active = true and revMid is not null
		and (publicTime is null or publicTime <= now());
	
create or replace view video_active_super as
	select * from video_info where active = true;

create or replace view danmu_active as
	select * from danmu_info where active = true;

-- drop all functions
drop function if exists verify_auth;
drop function if exists verify_video_req;
-- drop function if exists user_reg_check;
drop function if exists user_reg_sustc;
drop function if exists user_del_sustc;
drop function if exists add_follow;
drop function if exists get_user_info;
drop function if exists generate_unique_bv;
drop function if exists post_video;
drop function if exists del_video;
drop function if exists update_video;
drop function if exists search_video;
drop function if exists get_avg_view_rate;
drop function if exists get_hotspot;
drop function if exists rev_video;
drop function if exists coin_video;
drop function if exists like_video;
drop function if exists fav_video;
drop function if exists send_danmu;
drop function if exists display_danmu;
drop function if exists like_danmu;
drop function if exists recommend_next_video;
drop function if exists general_recommendations;
drop function if exists recommend_video_for_user;
drop function if exists recommend_friends;

-- function for VerifyAuth
-- create extension if not exists pgcrypto;

create or replace function verify_auth(
    _mid bigint,
    _pwd varchar(260),
    _qqid varchar(50),
    _wxid varchar(50)
)
    returns bigint as $$
    declare
        -- pwd_256 char(256);
        mid_mid bigint;
        qqid_mid bigint;
        wxid_mid bigint;
    begin
        if (_mid <= 0 or _pwd is null or _pwd = '') then
			mid_mid := null;
        else
            -- pwd_256 := cast(digest(_pwd, 'sha256') as char(256));
            mid_mid := (
	            select mid from user_active
	                where user_active.mid = _mid
	                    and user_active.pwd = _pwd -- pwd_256
	        );
	    end if;
        if (_qqid is null or _qqid = '') then
            qqid_mid := null;
		else
			qqid_mid := (
				select mid from user_active
					where user_active.qqid = _qqid
			);
		end if;
		if (_wxid is null or _wxid = '') then
			wxid_mid := null;
		else
	        wxid_mid := (
	            select mid from user_active
	                where user_active.wxid = _wxid
	        );
	    end if;
        if qqid_mid is not null and wxid_mid is not null
            and qqid_mid <> wxid_mid then
            -- raise notice 'OIDC via QQ and WeChat contradicts.';
            return -1;
        end if;
        if mid_mid is null and qqid_mid is null and wxid_mid is null then
            -- raise notice 'Authentication failed.';
            return -1;
        end if;
        if mid_mid is not null then
            return mid_mid;
        end if;
        if qqid_mid is not null then
            return qqid_mid;
        end if;
        if wxid_mid is not null then
            return wxid_mid;
        end if;
    end $$ language plpgsql;



-- function for VerifyVideoReq
create or replace function verify_video_req(
    _title text,
    _duration float8,
    _publicTime timestamp,
    _auth_mid bigint,
    _bv varchar(25)
)
    returns boolean as $$
    begin
        if _title is null or _title = '' then
            -- raise notice 'Title is null or empty.';
            return false;
        end if;
        if _duration < 10 then
            -- raise notice 'Duration is less than 10 seconds.';
            return false;
        end if;
        if _publicTime is null or _publicTime < now() then
            -- raise notice 'Publish time before current time.';
            return false;
        end if;
        if exists(select 1 from video_active_super
            where video_active_super.title = _title
                and video_active_super.ownMid = _auth_mid
                and video_active_super.bv <> _bv) then
            -- raise notice 'Title already exists.';
            return false;
        end if;
        return true;
    end $$ language plpgsql;

-- functions for UserServiceImpl

create or replace function user_reg_sustc(
    _name text,
    _sex varchar(10),
    _birthday varchar(10),
    _sign text,
    _pwd varchar(260),
    _qqid varchar(50),
    _wxid varchar(50)
)
    returns bigint as $$
    declare
        id bigint;
	    __birthday date;
		month int;
		day int;
    begin
        if _name is null or _name = '' then
            -- raise notice 'Name cannot be null or empty.';
            return -1;
        end if;
        if _pwd is null or _pwd = '' then
            -- raise notice 'Password cannot be null or empty.';
            return -1;
        end if;
        -- new.pwd := digest(new.pwd, 'sha256');
        if _sex is null or _sex = '' then
            -- raise notice 'Sex cannot be null.';
            return -1;
        end if;
		if _birthday is not null and not (_birthday ~ '^\\d{1,2}月\\d{1,2}日$') then
			-- raise notice 'Birthday format error.';
			return -1;
		end if;
        if (_birthday is not null and _birthday <> '') then
			begin
				__birthday := to_date(_birthday, 'MM月DD日');
			exception when others then
				-- raise notice 'Birthday format error.';
				return -1;
			end;
		end if;
		month := extract(month from __birthday);
		day := extract(day from __birthday);
		if month not between 1 and 12 or day not between 1 and 31 then
			-- raise notice 'Birthday format error.';
			return -1;
		end if;
        if _qqid is not null and _qqid <> '' and exists(select 1 from user_active where user_active.qqid = _qqid) then
            -- raise notice 'QQ used.';
            return -1;
        end if;
        if _wxid is not null and _wxid <> '' and exists(select 1 from user_active where user_active.wxid = _wxid) then
            -- raise notice 'WeChat used.';
            return -1;
        end if;
        begin
	        insert into user_info (name, sex, birthday, sign, pwd, qqid, wxid)
	            values (_name, _sex, _birthday, _sign, _pwd, _qqid, _wxid);
        exception when others then
            -- raise notice 'User registration failed.';
            return -1;
        end;
        id := (
			select max(mid) from user_active
				where user_active.name = _name
		);
		return id;
    end $$ language plpgsql;

create or replace function user_del_sustc(
    auth_mid bigint,
    auth_pwd text,
    auth_qqid varchar(50),
    auth_wxid varchar(50),
    _mid bigint
)
    returns boolean as $$
    declare
        real_mid bigint;
    begin
        real_mid := (select verify_auth(auth_mid, auth_pwd, auth_qqid, auth_wxid));
        if real_mid < 0 then
            -- raise notice 'Authentication failed.';
            return false;
        end if;
        if not exists(select 1 from user_active where user_active.mid = _mid) then
            -- raise notice 'User not found.';
            return false;
        end if;
        if real_mid = _mid or
            ((select identity from user_active where user_active.mid = real_mid) = 'SUPER')
            and ((select identity from user_active where user_active.mid = _mid) = 'USER') then
            update user_info set active = false where user_info.mid = _mid;
            update video_info set active = false where video_info.ownMid = _mid;
            update danmu_info set active = false where bv in (
                select bv from video_info where video_info.ownMid = _mid
            );
            update danmu_info set active = false where senderMid = _mid;
            return true;
        end if;
        return false;
    end $$ language plpgsql;

create or replace function add_follow(
    auth_mid bigint,
    auth_pwd varchar(260),
    auth_qqid varchar(50),
    auth_wxid varchar(50),
    followee_mid bigint
)
    returns boolean as $$
    declare
        real_mid bigint;
    begin
        real_mid := (select verify_auth(auth_mid, auth_pwd, auth_qqid, auth_wxid));
        if real_mid < 0 or real_mid = followee_mid then
            return false;
        end if;
        if not exists(select 1 from user_active where user_active.mid = followee_mid) then
			-- raise notice 'Followee not found.';
			return false;
		end if;
		if exists(select 1 from user_follow where star_mid = followee_mid and fan_mid = real_mid) then
			-- raise notice 'Followee already followed.';
			delete from user_follow where star_mid = followee_mid and fan_mid = real_mid;
			return false;
		else
			insert into user_follow (star_mid, fan_mid) values (followee_mid, real_mid);
			return true;
		end if;
	end $$ language plpgsql;

create or replace function get_user_info (_mid bigint) returns table(
	_coin int,
	following bigint[],
	follower bigint[],
	watched varchar(25)[],
	liked varchar(25)[],
	faved varchar(25)[],
	posted varchar(25)[]
) as $$
	begin
		return query
		select
			(select user_active.coin from user_active where user_active.mid = _mid) as _coin,
			(select array_agg(star_mid) from user_follow where fan_mid = _mid) as following,
			(select array_agg(fan_mid) from user_follow where star_mid = _mid) as follower,
			(select array_agg(bv) from user_watch_video where user_watch_video.mid = _mid) as watched,
			(select array_agg(bv) from user_like_video where user_like_video.mid = _mid) as liked,
			(select array_agg(bv) from user_fav_video where user_fav_video.mid = _mid) as faved,
			(select array_agg(bv) from video_info where ownMid = _mid) as posted;
	end $$ language plpgsql;



-- functions for VideoServiceImpl
create or replace function generate_unique_bv() returns text as $$
	declare
        new_uuid text;
	begin
	    loop
	        new_uuid := substring(gen_random_uuid()::text FROM 1 FOR 15);
	        if not exists(select 1 from video_info where bv = new_uuid) then
	            return 'BV' || new_uuid;
	        end if;
	    end loop;
	end $$ language plpgsql;

create or replace function post_video(
    auth_mid bigint,
    auth_pwd varchar(260),
    auth_qqid varchar(50),
    auth_wxid varchar(50),
    _title text,
    _descr text,
    _duration float8,
    _publicTime timestamp
)
    returns varchar(25) as $$
    declare
        bv text;
        real_mid bigint;
    begin
        real_mid := (select verify_auth(auth_mid, auth_pwd, auth_qqid, auth_wxid));
        if real_mid < 0 then
            -- raise notice 'Authentication failed.';
            return null;
        end if;
        if not verify_video_req(_title, _duration, _publicTime, real_mid, '') then
            -- raise notice 'Video verification failed.';
            return null;
        end if;
        bv := generate_unique_bv();
        insert into video_info (bv, title, ownMid, commitTime, publicTime, duration, descr)
            values (bv, _title, real_mid, now(), _publicTime, _duration, _descr);
        return bv;
    end $$ language plpgsql;

create or replace function del_video(
    auth_mid bigint,
    auth_pwd char(256),
    auth_qqid varchar(50),
    auth_wxid varchar(50),
    _bv varchar(25)
)
    returns boolean as $$
    declare
        real_mid bigint;
    begin
        real_mid := (select verify_auth(auth_mid, auth_pwd, auth_qqid, auth_wxid));
        if real_mid < 0 then
            -- raise notice 'Authentication failed.';
            return false;
        end if;
        if not exists(select 1 from video_active_super where bv = _bv) then
            -- raise notice 'Video not found.';
            return false;
        end if;
        if real_mid = (select ownMid from video_active_super where bv = _bv) or
            ((select identity from user_active where user_active.mid = real_mid) = 'SUPER') then
            update video_info set active = false where bv = _bv;
            update danmu_info set active = false where bv = _bv;
            return true;
        end if;
        return false;
    end $$ language plpgsql;

create or replace function update_video(
    auth_mid bigint,
    auth_pwd char(256),
    auth_qqid varchar(50),
    auth_wxid varchar(50),
    _bv varchar(25),
    _title text,
    _descr text,
    _duration float8,
    _publicTime timestamp
)
    returns boolean as $$
    declare
        real_mid bigint;
    begin
        real_mid := (select verify_auth(auth_mid, auth_pwd, auth_qqid, auth_wxid));
        if real_mid < 0 then
            -- raise notice 'Authentication failed.';
            return false;
        end if;
        if not exists(select 1 from video_active_super
            where bv = _bv and ownMid = real_mid and duration = _duration) then
            -- raise notice 'Video not found / duration changed.';
            return false;
        end if;
        if not verify_video_req(_title, _duration, _publicTime, real_mid, _bv) then
            -- raise notice 'Video verification failed.';
            return false;
        end if;
        if exists(select 1 from video_active_super
            where bv = _bv and title = _title and descr = _descr
                and publicTime = _publicTime) then
            -- raise notice 'Nothing changes.';
            return false;
        end if;
        update video_info set
            title = _title,
            descr = _descr,
            duration = _duration,
            publicTime = _publicTime
            where bv = _bv;
        if (select revMid from video_active_super where bv = _bv) is not null then
			update video_info set revMid = null, reviewTime = null where bv = _bv;
			return true;
		end if;
        return false;
    end $$ language plpgsql;

create or replace function search_video(
    auth_mid bigint,
    auth_pwd varchar(260),
    auth_qqid varchar(50),
    auth_wxid varchar(50),
    keywords text,
    page_size int,
    page_num int
)
    returns varchar(25)[] as $$
	declare
		real_mid bigint;
    begin
        real_mid := (select verify_auth(auth_mid, auth_pwd, auth_qqid, auth_wxid));
        if real_mid < 0 then
            -- raise notice 'Authentication failed.';
            return null;
        end if;
        return (
            with word_set as (
                select regexp_split_to_table(keywords, E'\\\\s+') as word
			)
	        select array_agg(ans) from (
		        select tmp4.bv as ans from
		            (select user_watch_video.bv, count(*) as cnt
		                from user_watch_video group by user_watch_video.bv
		            ) as watch_cnt
		        join
		            (select tmp1.bv, revMid, publicTime, ownMid, sum(
				            (select regexp_count(tmp1.title, word, 1, 'i')) +
				            (select regexp_count(coalesce(tmp1.descr, ''), word, 1, 'i')) +
				            (select regexp_count(tmp1.name, word, 1, 'i'))
			            ) as relevance
		                from (
		                    (select video_active_super.bv, title, descr, ownMid, name, revMid, publicTime
		                        from (video_active_super join user_active on ownMid = mid)) tmp2
		                    cross join word_set
		                ) as tmp1
		                group by tmp1.bv, revMid, publicTime, ownMid
		            ) tmp4
		        on watch_cnt.bv = tmp4.bv
		        where relevance > 0 and ((tmp4.revMid is not null and tmp4.publicTime < now())
		            or tmp4.ownMid = real_mid
		            or (select identity from user_active where mid = real_mid) = 'SUPER')
		        group by tmp4.bv, relevance, watch_cnt.cnt
		        order by relevance desc, cnt desc
		        limit page_size offset ((page_num - 1) * page_size)
		    ) as tmpx
		);
    end $$ language plpgsql;

create or replace function get_avg_view_rate(_bv varchar(25))
    returns double precision as $$
    declare
        _cnt int;
        _avg double precision;
    begin
        if not exists(select 1 from video_active_super where bv = _bv) then
            -- raise notice 'Video not found.';
            return -1;
        end if;
        select count(*), avg(lastpos) into _cnt, _avg from user_watch_video where bv = _bv;
        if _cnt = 0 then
            -- raise notice 'No one has watched this video.';
            return -1;
        end if;
        return _avg / (select duration from video_active_super where bv = _bv);
    end $$ language plpgsql;

create or replace function get_hotspot(_bv varchar(25))
    returns table(chunkId int[]) as $$
    begin
        if not exists(select 1 from video_active_super where bv = _bv) then
            -- raise notice 'Video not found.';
            return query select array_agg(0) where false;
        end if;
        return query
        select array_agg(cast(hotspot.chunkId as int)) from (
            select danmu_cnt.chunkId, cnt, max(cnt) over() as maxx from (
                select floor(showtime / 10) as chunkId, count(*) as cnt
                    from danmu_info where bv = _bv group by chunkId
            ) as danmu_cnt
        ) as hotspot where cnt = maxx and maxx <> 0;
    end $$ language plpgsql;

create or replace function rev_video(
    auth_mid bigint,
    auth_pwd char(256),
    auth_qqid varchar(50),
    auth_wxid varchar(50),
    _bv varchar(25)
)
    returns boolean as $$
    declare
        _ownMid bigint;
        _revMid bigint;
        real_mid bigint;
    begin
        real_mid := (select verify_auth(auth_mid, auth_pwd, auth_qqid, auth_wxid));
        if real_mid < 0 then
            -- raise notice 'Authentication failed.';
            return false;
        end if;
        if not exists(select 1 from video_active_super where bv = _bv) then
            -- raise notice 'Video not found.';
            return false;
        end if;
        if (select identity from user_active where mid = real_mid) = 'USER' then
            -- raise notice 'Insufficient permission.';
            return false;
        end if;
        select ownMid, revMid into _ownMid, _revMid from video_active_super where bv = _bv;
        if _ownMid = real_mid then
            -- raise notice 'Cannot review own video.';
            return false;
        end if;
        if _revMid is not null then
            -- raise notice 'Video has been reviewed.';
            return false;
        end if;
        begin
            update video_info
                set revMid = real_mid, reviewTime = now()
                where bv = _bv;
        exception when others then
            -- raise notice 'Review failed.';
            return false;
        end;
        return true;
    end $$ language plpgsql;

create or replace function coin_video(
    auth_mid bigint,
    auth_pwd char(256),
    auth_qqid varchar(50),
    auth_wxid varchar(50),
    _bv varchar(25)
)
    returns boolean as $$
    declare
        _ownMid bigint;
        real_mid bigint;
    begin
        real_mid := (select verify_auth(auth_mid, auth_pwd, auth_qqid, auth_wxid));
        if real_mid < 0 then
            -- raise notice 'Authentication failed.';
            return false;
        end if;
        if (select identity from user_active where mid = real_mid) = 'USER' then
            if not exists(select 1 from video_active where bv = _bv) then
                -- raise notice 'Video doesn''t exist.';
                return false;
            end if;
        else
            if not exists(select 1 from video_active_super where bv = _bv) then
                -- raise notice 'Video doesn''t exist.';
                return false;
            end if;
        end if;
        _ownMid := (select ownMid from video_active_super where bv = _bv);
        if (_ownMid = real_mid) then
            -- raise notice 'Cannot coin your own video';
            return false;
        end if;
        begin
            insert into user_coin_video (mid, bv) values (real_mid, _bv);
            update user_info set coin = coin - 1 where mid = real_mid;
        exception when others then
            -- raise notice 'Coin failed.';
            return false;
        end;
        return true;
    end $$ language plpgsql;

create or replace function like_video(
    auth_mid bigint,
    auth_pwd char(256),
    auth_qqid varchar(50),
    auth_wxid varchar(50),
    _bv varchar(25)
)
    returns boolean as $$
    declare
        _ownMid bigint;
        real_mid bigint;
    begin
        real_mid := (select verify_auth(auth_mid, auth_pwd, auth_qqid, auth_wxid));
        if real_mid < 0 then
            -- raise notice 'Authentication failed.';
            return false;
        end if;
        if (select identity from user_active where mid = real_mid) = 'USER' then
            if not exists(select 1 from video_active where bv = _bv) then
                -- raise notice 'Video doesn''t exist.';
                return false;
            end if;
        else
            if not exists(select 1 from video_active_super where bv = _bv) then
                -- raise notice 'Video doesn''t exist.';
                return false;
            end if;
        end if;
        _ownMid := (select ownMid from video_active_super where bv = _bv);
        if (_ownMid = real_mid) then
            -- raise notice 'Cannot coin your own video';
            return false;
        end if;
        begin
            insert into user_like_video (mid, bv)
                values (real_mid, _bv);
        exception when others then
            -- raise notice 'Like failed.';
            return false;
        end;
        return true;
    end $$ language plpgsql;

create or replace function fav_video(
    auth_mid bigint,
    auth_pwd char(256),
    auth_qqid varchar(50),
    auth_wxid varchar(50),
    _bv varchar(25)
)
    returns boolean as $$
    declare
        _ownMid bigint;
        real_mid bigint;
    begin
        real_mid := (select verify_auth(auth_mid, auth_pwd, auth_qqid, auth_wxid));
        if real_mid < 0 then
            -- raise notice 'Authentication failed.';
            return false;
        end if;
        if (select identity from user_active where mid = real_mid) = 'USER' then
            if not exists(select 1 from video_active where bv = _bv) then
                -- raise notice 'Video doesn''t exist.';
                return false;
            end if;
        else
            if not exists(select 1 from video_active_super where bv = _bv) then
                -- raise notice 'Video doesn''t exist.';
                return false;
            end if;
        end if;
        _ownMid := (select ownMid from video_active_super where bv = _bv);
        if (_ownMid = real_mid) then
            -- raise notice 'Cannot collect your own video';
            return false;
        end if;
        begin
            insert into user_fav_video (mid, bv)
                values (real_mid, _bv);
        exception when others then
            -- raise notice 'Collection failed.';
            return false;
        end;
        return true;
    end $$ language plpgsql;



-- functions for DanmuServiceImpl
create or replace function send_danmu (
    auth_mid bigint,
    auth_pwd varchar(260),
    auth_qqid varchar(50),
    auth_wxid varchar(50),
    _bv varchar(25),
    _content text,
    show_time float8
)
    returns bigint as $$
    declare
        _danmu_id bigint;
        real_mid bigint;
        time timestamp;
    begin
        real_mid := (select verify_auth(auth_mid, auth_pwd, auth_qqid, auth_wxid));
        if real_mid < 0 then
            -- raise notice 'Authentication failed.';
            return -1;
        end if;
        if not exists(select 1 from video_active where bv = _bv) then
            -- raise notice 'Video doesn''t exist.';
            return -1;
        end if;
        if not exists(select 1 from user_watch_video
            where mid = real_mid and bv = _bv) then
            -- raise notice 'You should watch the video first.';
            return -1;
        end if;
        if (show_time not between 0 and (select duration from video_active where _bv = bv)) then
            -- raise notice 'Invaild show_time.';
            return -1;
        end if;
        time := now();
        begin
            insert into danmu_info (bv, senderMid, showtime, content, postTime)
                values (_bv, real_mid, show_time, _content, time);
        exception when others then
                -- raise notice 'Send danmu failed.';
                return -1;
        end;
        select danmu_id into _danmu_id from danmu_info
            where bv = _bv and senderMid = real_mid and postTime = time;
        return _danmu_id;
    end $$ language plpgsql;

create or replace function display_danmu (
    _bv text,
    start_time float8,
    end_time float8,
    _filter boolean
)
    returns bigint[] as $$
    begin
        if not exists(select 1 from video_active_super where bv = _bv) then
            -- raise notice 'Video not found.';
            return null;
        end if;
        if (start_time > end_time or start_time < 0
            or end_time > (select duration from video_active_super where bv = _bv)) then
            -- raise notice 'Time is invalid.';
            return null;
        end if;
        if _filter then
            return (
	            with allDanmu as (
	                select danmu_active.danmu_id, content, postTime from danmu_active
	                    where danmu_active.bv = _bv and showTime between start_time and end_time
	            )
	            select array_agg(DPP.danmu_id) from (
	                select allDanmu.danmu_id, postTime,
	                    min(posttime) over(partition by content) as firstPosted
	                from allDanmu
	            ) as DPP where postTime = firstPosted
	        );
        else
            return (
	            select array_agg(danmu_active.danmu_id) from danmu_active
	                where bv = _bv and showTime between start_time and end_time
	        );
        end if;
    end $$ language plpgsql;

create or replace function like_danmu (
    auth_mid bigint,
    auth_pwd varchar(260),
    auth_qqid varchar(50),
    auth_wxid varchar(50),
    _danmu_id bigint
)
    returns boolean as $$
    declare
        real_mid bigint;
    begin
        real_mid := (select verify_auth(auth_mid, auth_pwd, auth_qqid, auth_wxid));
        if real_mid < 0 then
            -- raise notice 'Authentication failed.';
            return false;
        end if;
        if not exists(select 1 from danmu_active where danmu_id = _danmu_id) then
            -- raise notice 'Danmu not found.';
            return false;
        end if;
        if not exists(select 1 from user_watch_video
            where mid = real_mid and bv = (select bv from danmu_info where danmu_id = _danmu_id)) then
            -- raise notice 'You should watch the video first.';
            return false;
        end if;
        begin
            insert into user_like_danmu(danmu_id, mid)
                values (_danmu_id, real_mid);
        exception when others then
            -- raise notice 'Like danmu failed.';
            return false;
        end;
        return true;
    end $$ language plpgsql;



-- functions for RecommenderServiceImpl
create or replace function recommend_next_video (_bv varchar(25))
    returns varchar(25)[] as $$
    begin
        if (select exists(select 1 from video_active_super where bv = _bv) = false) then
            -- raise notice 'Video not found.';
            return null;
        end if;
        return (
	        select array_agg(rec_next_tmp.bv) from (
	                select user_watch_video.bv from user_watch_video
	                    where mid in (
	                        select mid from user_watch_video where user_watch_video.bv = _bv
	                    ) and user_watch_video.bv != _bv
				    group by user_watch_video.bv
				    order by count(user_watch_video.bv) desc, user_watch_video.bv
				    limit 5
	            ) rec_next_tmp
		);
    end $$ language plpgsql;

create or replace function general_recommendations (
	page_size int,
	page_num int
)
	returns varchar(25)[] as $$
    begin
        return (
            with auxCnt as (
                select user_watch_video.bv, count(*) as cnt from user_watch_video group by user_watch_video.bv
            )
            select array_agg(tmp.bv) from (
                select video_active_super.bv, (
                    case
                        when cnt = 0 then 0
                        else ((coalesce(cnt_like, 0) + coalesce(cnt_coin, 0) + coalesce(cnt_fav, 0)
                            + coalesce(cnt_danmu, 0) + coalesce(cnt_watch, 0) / video_active_super.duration) / cnt)
                    end) as score
                    from video_active_super join auxCnt on video_active_super.bv = auxCnt.bv
                        left join (select count(*) as cnt_like, bv from user_like_video group by bv) as likeCnt
							on video_active_super.bv = likeCnt.bv
						left join (select count(*) as cnt_coin, bv from user_coin_video group by bv) as coinCnt
							on video_active_super.bv = coinCnt.bv
						left join (select count(*) as cnt_fav, bv from user_fav_video group by bv) as favCnt
							on video_active_super.bv = favCnt.bv
						left join (select count(*) as cnt_danmu, bv from danmu_info group by bv) as danmuCnt
							on video_active_super.bv = danmuCnt.bv
						left join (select sum(lastpos) as cnt_watch, bv from user_watch_video group by bv) as watchCnt
							on video_active_super.bv = watchCnt.bv
                    order by score desc
                    limit page_size offset (page_num - 1) * page_size
            ) as tmp
        );
    end; $$ language plpgsql;

create or replace function recommend_video_for_user (
    auth_mid bigint,
    auth_pwd varchar(260),
    auth_qqid varchar(50),
    auth_wxid varchar(50),
    page_size int,
    page_num int
)
    returns varchar(25)[] as $$
    declare
    	real_mid bigint;
    begin
        real_mid := (select verify_auth(auth_mid, auth_pwd, auth_qqid, auth_wxid));
        if real_mid < 0 then
            -- raise notice 'Authentication failed.';
            return null;
        end if;
        if (select exists(
            select star_mid as f_mid from user_follow where fan_mid = real_mid
			intersect
			select fan_mid as f_mid from user_follow where star_mid = real_mid) = false) then
			return general_recommendations(page_size, page_num);
		end if;
        if (select identity from user_active where mid = real_mid) = 'USER' then
			return (
				select array_agg(bv) from (
			        select validBv1.bv from (
		                select bv, count(*) as cnt from user_watch_video
		                    where mid in (select f_mid from (
		                        select star_mid as f_mid from user_follow where fan_mid = real_mid
							    intersect
							    select fan_mid as f_mid from user_follow where star_mid = real_mid
		                    ) as fr1) and bv not in (
		                        select bv from user_watch_video where mid = real_mid
		                    ) group by bv) validBv1
		                join video_active_super on video_active_super.bv = validBv1.bv
		                join user_active on user_active.mid = video_active_super.ownMid
                        where exists (select 1 from video_active where bv = validBv1.bv)
                            or exists (select 1 from video_active_super where bv = validBv1.bv and ownmid = real_mid)
		                order by cnt desc, level desc, publicTime desc
			            limit page_size offset (page_num - 1) * page_size
			    ) as tmp_rvfu1
		    );
        else
            return (
                select array_agg(bv) from (
		            select validBv2.bv from (
			            select bv, count(*) as cnt from user_watch_video
			                where mid in (select f_mid from (
		                        select star_mid as f_mid from user_follow where fan_mid = real_mid
							    intersect
							    select fan_mid as f_mid from user_follow where star_mid = real_mid
		                    ) as fr2) and bv not in (
			                    select bv from user_watch_video where mid = real_mid
			                ) group by bv) validBv2
			            join video_active_super on video_active_super.bv = validBv2.bv
			            join user_active on user_active.mid = video_active_super.ownMid
			            order by cnt desc, level desc, publicTime desc
			            limit page_size offset (page_num - 1) * page_size
			    ) as tmp_rvfu2
		    );
        end if;
    end $$ language plpgsql;

create or replace function recommend_friends (
    auth_mid bigint,
    auth_pwd varchar(260),
    auth_qqid varchar(50),
    auth_wxid varchar(50),
    page_size int,
    page_num int
)
    returns bigint[] as $$
    declare
    	real_mid bigint;
    begin
        real_mid := (select verify_auth(auth_mid, auth_pwd, auth_qqid, auth_wxid));
        if real_mid < 0 then
            -- raise notice 'Authentication failed.';
            return null;
        end if;
        return (
	        with newFriends as (
	            select uf.fan_mid as uf_mid, count(*) as cnt from
	            (select user_follow.star_mid from user_follow where user_follow.fan_mid = real_mid) myFollowings
	                join user_follow as uf on myFollowings.star_mid = uf.star_mid
	            where uf.fan_mid not in (select user_follow.star_mid from user_follow where user_follow.fan_mid = real_mid)
	                and uf.fan_mid <> real_mid
	            group by uf.fan_mid
	        )
	        select array_agg(tmp_rfs.mid) from (
	            select user_active.mid from newFriends join user_active
	                on newFriends.uf_mid = user_active.mid
	            order by cnt desc, level desc, mid limit page_size offset (page_num - 1) * page_size
	        )tmp_rfs
		);
    end $$ language plpgsql;
		""";
		try (Connection conn = dataSource.getConnection();
			PreparedStatement stmt = conn.prepareStatement(createFunctions)) {
			stmt.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

		String addUserTrigger = "alter system set full_page_writes = off;";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(addUserTrigger)) {
			stmt.execute();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * The following code is just a quick example of using jdbc datasource.
	 * Practically, the code interacts with database is usually written in a DAO layer.
	 * Reference: <a href="https://www.baeldung.com/java-dao-pattern">Data Access Object pattern</a>
	 */

	@Override
	public void truncate() {
		// You can use the default truncate script provided by us in most cases,
		// but if it doesn't work properly, you may need to modify it.

		String sql = """
DO $$
DECLARE
	tables CURSOR FOR
		SELECT tablename
		FROM pg_tables
		WHERE schemaname = 'public';
BEGIN
	FOR t IN tables
	LOOP
		EXECUTE 'TRUNCATE TABLE ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';
	END LOOP;
END $$;
		""";

		try (Connection conn = dataSource.getConnection();
			PreparedStatement stmt = conn.prepareStatement(sql)) {
			stmt.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Sums up two numbers via Postgres.
	 * This method only demonstrates how to access database via JDBC.
	 *
	 * @param a the first number
	 * @param b the second number
	 * @return the sum of two numbers
	 */
	@Override
	public Integer sum(int a, int b) {
		String sql = "SELECT ?+?";

		try (Connection conn = dataSource.getConnection();
			PreparedStatement stmt = conn.prepareStatement(sql)) {
			stmt.setInt(1, a);
			stmt.setInt(2, b);
			log.info("SQL: {}", stmt);

			ResultSet rs = stmt.executeQuery();
			rs.next();
			return rs.getInt(1);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
