package alphatier.manager;

import alphatier.manager.cluster.ClusterModule;
import com.google.common.eventbus.EventBus;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.Matchers;
import com.google.inject.spi.InjectionListener;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;

public class MainModule extends AbstractModule {
    private MainModule() {
    }

    @Override
    protected void configure() {
        final EventBus eventBus = new EventBus("alphatier-eventbus");
        bind(EventBus.class).asEagerSingleton();
        bindListener(Matchers.any(), new TypeListener() {
            @Override
            public <I> void hear(@SuppressWarnings("unused") final TypeLiteral<I> typeLiteral, final TypeEncounter<I> typeEncounter) {
                typeEncounter.register(new InjectionListener<I>() {
                    @Override public void afterInjection(final I instance) {
                        eventBus.register(instance);
                    }
                });
            }
        });
        
        install(new ClusterModule());
    }

    public static void main(final String[] args) {
        final Injector injector = Guice.createInjector(new MainModule());
        final EventBus eventBus = injector.getBinding(Key.get(EventBus.class));
    }
}
