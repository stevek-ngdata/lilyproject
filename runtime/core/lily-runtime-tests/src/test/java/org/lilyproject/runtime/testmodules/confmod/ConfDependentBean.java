package org.lilyproject.runtime.testmodules.confmod;

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
