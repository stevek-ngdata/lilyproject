package org.lilyproject.lilyservertestfw.launcher;

public interface LilyLauncherMBean {
    void resetLilyState();

    String getSolrHome();

    void restartSolr() throws Exception;
}
