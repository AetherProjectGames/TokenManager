package me.realized.tokenmanager.data.database;

import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalDouble;
import java.util.function.Consumer;
import java.util.function.Function;
import me.realized.tokenmanager.command.commands.subcommands.OfflineCommand.ModifyType;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import org.bukkit.event.player.AsyncPlayerPreLoginEvent;

public interface Database {

    boolean isOnlineMode();

    /**
     * Checks and creates the table for the plugin if it does not exist.
     *
     * @throws Exception If the found table does not have the key column matching with the server mode (UUID for online/name for offline)
     */
    void setup() throws Exception;

    /**
     * Gets the cached balance of the player.
     *
     * @param player Player to get the data
     * @return instance of {@link OptionalDouble} with the player's token balance if found, otherwise empty
     */
    OptionalDouble get(final Player player);

    void get(final String uuid, final String username, final Consumer<OptionalDouble> onLoad, final Consumer<String> onError, final boolean create);

    void set(final Player player, final double value);

    void set(final String uuid, final String username, final ModifyType type, final double amount, final double balance, final boolean silent, final Runnable onDone, final Consumer<String> onError);

    void shutdown() throws Exception;

    /**
     * Returns top balances. Must be called synchronously!
     *
     * @param limit amount of the rows to be returned
     * @param onLoad Consumer to call once data is retrieved
     */
    void ordered(final int limit, final Consumer<List<TopElement>> onLoad);

    void transfer(final CommandSender sender, final Consumer<String> onError);

    class TopElement {

        private final double tokens;
        private String uuid;
        private String username;

        TopElement(final String uuid, final String username, final double tokens) {
            this.uuid = uuid;
            this.username = username;
            this.tokens = tokens;
        }

        public String getKey() {
            return uuid;
        }

        void setKey(final String key) {
            this.uuid = key;
        }

        public double getTokens() {
            return tokens;
        }
    }
}
