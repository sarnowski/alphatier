package alphatier.manager.cluster;

import com.google.inject.AbstractModule;

public class ClusterModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(Cluster.class).to(JGroupsCluster.class).asEagerSingleton();
    }
}
