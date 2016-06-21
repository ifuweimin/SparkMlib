<%@ page import="com.test.HbaseUtils" %>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%
    String q = request.getParameter("q");
    if(q == null || q.equals("")){
        out.print("");
    }
    q = new String(q.getBytes("ISO-8859-1"), "UTF-8");

    System.out.print("q:"+q);
    String result = HbaseUtils.getResult(java.net.URLEncoder.encode(q,"UTF-8"));
    out.println("{"+result+"}");


%>