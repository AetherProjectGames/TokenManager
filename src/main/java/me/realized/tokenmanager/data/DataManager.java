package me.realized.tokenmanager.data;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.OptionalDouble;
import java.util.UUID;
import java.util.function.Consumer;
import lombok.Getter;
import me.realized.tokenmanager.TokenManagerPlugin;
import me.realized.tokenmanager.command.commands.subcommands.OfflineCommand.ModifyType;
import me.realized.tokenmanager.data.database.Database;
import me.realized.tokenmanager.data.database.Database.TopElement;
import me.realized.tokenmanager.data.database.MySQLDatabase;
import me.realized.tokenmanager.util.Loadable;
import me.realized.tokenmanager.util.Log;
import me.realized.tokenmanager.util.StringUtil;
import org.bukkit.Bukkit;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.player.AsyncPlayerPreLoginEvent;
import org.bukkit.event.player.AsyncPlayerPreLoginEvent.Result;
import org.bukkit.event.player.PlayerQuitEvent;
import org.bukkit.scheduler.BukkitScheduler;

public class DataManager implements Loadable, Listener {

    private final TokenManagerPlugin plugin;

    private Database database;

    @Getter
    private List<TopElement> topCache = new ArrayList<>();
    private Integer topTask, updateInterval;
    private long lastUpdateMillis;

    private final Multimap<UUID, QueuedCommand> queuedCommands = LinkedHashMultimap.create();

    public DataManager(final TokenManagerPlugin plugin) {
        this.plugin = plugin;
        Bukkit.getPluginManager().registerEvents(this, plugin);
    }

    @Override
    public void handleLoad() throws Exception {
        this.database = new MySQLDatabase(plugin);
        final boolean online = database.isOnlineMode();
        Log.info("===============================================");
        Log.info("TokenManager has detected your server as " + (online ? "online" : "offline") + " mode.");
        Log.info("DataManager will operate with " + (online ? "UUID" : "Username") + "s.");
        Log.info("If your server is NOT in " + (online ? "online" : "offline") + " mode, please manually set online-mode in TokenManager's config.yml.");
        Log.info("===============================================");
        database.setup();

        topTask = plugin.doSyncRepeat(() -> database.ordered(10, args -> plugin.doSync(() -> {
            lastUpdateMillis = System.currentTimeMillis();
            topCache = args;
        })), 0L, 20L * 60L * getUpdateInterval());
    }

    @Override
    public void handleUnload() throws Exception {
        if (topTask != null) {
            final BukkitScheduler scheduler = Bukkit.getScheduler();

            if (scheduler.isCurrentlyRunning(topTask) || scheduler.isQueued(topTask)) {
                scheduler.cancelTask(topTask);
            }
        }

        database.shutdown();
        database = null;
    }

    public OptionalDouble get(final Player player) {
        return database != null ? database.get(player) : OptionalDouble.empty();
    }

    public void set(final Player player, final double amount) {
        if (database != null) {
            database.set(player, amount);
        }
    }

    public void get(final  String uuid, final String username, final Consumer<OptionalDouble> onLoad, final Consumer<String> onError) {
        if (database != null) {
            database.get(uuid, username, onLoad, onError, false);
        }
    }

    public void set(final String uuid, final String username, final ModifyType type, final double amount, final double balance, final boolean silent, final Runnable onDone,
        final Consumer<String> onError) {
        if (database != null) {
            database.set(uuid, username, type, amount, balance, silent, onDone, onError);
        }
    }

    public void transfer(final CommandSender sender, final Consumer<String> onError) {
        if (database != null) {
            database.transfer(sender, onError);
        }
    }

    public void queueCommand(final Player player, final ModifyType type, final double amount, final boolean silent) {
        queuedCommands.put(player.getUniqueId(), new QueuedCommand(type, amount, silent));
    }

    private int getUpdateInterval() {
        if (updateInterval != null) {
            return updateInterval;
        }

        return (updateInterval = plugin.getConfiguration().getBalanceTopUpdateInterval()) < 1 ? 1 : updateInterval;
    }

    public String getNextUpdate() {
        return StringUtil.format((lastUpdateMillis + 60000L * getUpdateInterval() - System.currentTimeMillis()) / 1000);
    }

    @EventHandler
    public void on(final PlayerQuitEvent event) {
        final Player player = event.getPlayer();
        queuedCommands.asMap().remove(player.getUniqueId());
    }

    private class QueuedCommand {

        private final ModifyType type;
        private final double amount;
        private final boolean silent;

        QueuedCommand(final ModifyType type, final double amount, final boolean silent) {
            this.type = type;
            this.amount = amount;
            this.silent = silent;
        }
    }
}
