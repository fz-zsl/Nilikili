package io.sustc.service.impl;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public class EncryptSHA256 {
	public static String encrypt(String str) {
		int chunkSize = 64;
		String result = "";
		for (int i = 0; i < str.length(); i += chunkSize) {
			String chunk = str.substring(i, Math.min(str.length(), i + chunkSize));
			result += hastToString(hashString(chunk));
		}
		return result;
	}

	private static String hastToString(byte[] hash) {
		String result = "";
		for (byte b : hash) {
			result += Integer.toString((b & 0xff) + 0x100, 16).substring(1);
		}
		return result;
	}

	private static byte[] hashString(String str) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			return digest.digest(str.getBytes(StandardCharsets.UTF_8));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}