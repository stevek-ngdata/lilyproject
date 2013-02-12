package org.kauriproject.runtime.test.testmodules.confmod;

import org.kauriproject.conf.Conf;

public class ConfDependentBean {
    private Conf conf;

    public ConfDependentBean(Conf conf) {
        this.conf = conf;
    }

    public Conf getConf() {
        return conf;
    }
}
