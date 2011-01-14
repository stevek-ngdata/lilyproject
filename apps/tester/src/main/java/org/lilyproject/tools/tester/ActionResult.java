package org.lilyproject.tools.tester;

public class ActionResult {
    boolean success;
    Object object;
    double duration;

    public ActionResult(boolean success, Object object, double duration) {
        this.success = success;
        this.object = object;
        this.duration = duration;
    }
}