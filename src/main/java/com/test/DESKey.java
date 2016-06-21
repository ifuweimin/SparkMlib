package com.test;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import java.security.SecureRandom;

public class DESKey {
	
	private static Cipher getDES(String key, boolean b) throws Exception {
		SecureRandom sr = new SecureRandom();
		DESKeySpec dks = new DESKeySpec(key.getBytes());
		SecretKeyFactory keyfactory = SecretKeyFactory.getInstance("DES");
		SecretKey skey = keyfactory.generateSecret(dks);
		Cipher cipher = Cipher.getInstance("DES");
		if(b){  // 加密
			cipher.init(Cipher.ENCRYPT_MODE, skey, sr);
		}else{  //解密
			cipher.init(Cipher.DECRYPT_MODE, skey, sr);
		}
		
		return cipher;
	}

	public static String encrypt(String txt, String key){
		String r = null;
		try{
			Cipher cipher = getDES(key,true);
			byte[] b = cipher.doFinal(txt.getBytes());
			r = byte2hex(b);
			
		}catch(Exception e){
			System.out.println(e.getMessage());
		}
		return r;
		
	}

	public static String decrypt(String d, String key){
		String r = null;
		try{
			Cipher cipher = getDES(key,false);
			byte[] b = cipher.doFinal(hex2byte(d));
			r = new String(b);
		}catch(Exception e){
			System.out.println(e.getMessage());
		}
		return r;
	}
	
	private static byte[] hex2byte(String d) throws IllegalArgumentException {
		if(d.length() %2 != 0){
			throw new IllegalArgumentException();
		}
		char[] arr = d.toCharArray();
		byte[] b = new byte[d.length()/2 ];
		for(int i = 0,j = 0;i< d.length(); i++,j++){
			String wap = "" + arr[i++] + arr[i];
			int byteint = Integer.parseInt(wap, 16) & 0xFF;
			b[j] = new Integer(byteint).byteValue();
		}
		return b;
	}

	private static String byte2hex(byte[] b){
		StringBuffer sb = new StringBuffer();
		for (int n = 0; n < b.length; n++) {
			String stmp = (Integer.toHexString(b[n] & 0XFF));
			if (stmp.length() == 1) {
				sb.append("0" + stmp);
			} else {
				sb.append(stmp);
			}
		}
		return sb.toString();
	}

	
}

