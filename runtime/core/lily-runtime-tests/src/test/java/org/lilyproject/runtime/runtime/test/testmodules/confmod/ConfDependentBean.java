package org.lilyproject.runtime.runtime.test.testmodules.confmod;

import org.lilyproject.runtime.conf.Conf;

public class ConfDependentBean {
    private Conf conf;

    public ConfDependentBean(Conf conf) {
        this.conf = conf;
    }

    public Conf getConf() {
        return conf;
    }
}
