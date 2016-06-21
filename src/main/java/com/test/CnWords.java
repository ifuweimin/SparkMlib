package com.test;

import jeasy.analysis.MMAnalyzer;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by fuweimin on 2016/6/21.
 */
public class CnWords {

	public static List<String> parse(String s) throws IOException {
		MMAnalyzer analyzer = new MMAnalyzer();
		TokenStream source = analyzer.tokenStream("", new StringReader(s));
		source.reset();
		CachingTokenFilter buffer = new CachingTokenFilter(source);
		CharTermAttribute termAtt = null;

		if (buffer.hasAttribute(CharTermAttribute.class)) {
			termAtt = buffer.getAttribute(CharTermAttribute.class);
		}
		int nPerCount = 0 ;
		String text ;
		boolean bnext ;
		List<String> list = new ArrayList<>();
		while (true) {
			try {
				bnext = buffer.incrementToken() ;
			} catch (IOException e) {
				bnext = false ;
			}
			if (!bnext)
				break;
			text = termAtt.toString() ;
			if (!text.equals("")){
				list.add(text);
				nPerCount ++ ;
			}
		}
		return list;
	}

	public static void main(String[] args) throws IOException {
		List<String> a = parse("商品的  服务");
		List<String> b = parse("商品服务");
		System.out.println(a.equals(b));
	}
}
