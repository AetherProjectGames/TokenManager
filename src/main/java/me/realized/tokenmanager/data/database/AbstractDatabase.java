package me.realized.tokenmanager.data.database;

import java.util.List;
import java.util.OptionalDouble;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import me.realized.tokenmanager.TokenManagerPlugin;
import me.realized.tokenmanager.util.profile.ProfileUtil;
import org.bukkit.entity.Player;

public abstract class AbstractDatabase implements Database {

    protected final TokenManagerPlugin plugin;

    AbstractDatabase(final TokenManagerPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public boolean isOnlineMode() {
        return true;
    }

    OptionalDouble from(final Double value) {
        return value != null ? OptionalDouble.of(value) : OptionalDouble.empty();
    }

    String from(final Player player) {
        return player.getUniqueId().toString();
    }

    void replaceNames(final List<TopElement> list, final Consumer<List<TopElement>> callback) {
        ProfileUtil.getNames(list.stream().map(element -> UUID.fromString(element.getKey())).collect(Collectors.toList()), result -> {
            for (final TopElement element : list) {
                final String name = result.get(UUID.fromString(element.getKey()));

                if (name == null) {
                    element.setKey("&cFailed to get name!");
                    continue;
                }

                element.setKey(name);
            }

            callback.accept(list);
        });
    }
}
