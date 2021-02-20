package me.realized.tokenmanager.data.database;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.DataOutput;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.Getter;
import me.realized.tokenmanager.TokenManagerPlugin;
import me.realized.tokenmanager.command.commands.subcommands.OfflineCommand.ModifyType;
import me.realized.tokenmanager.config.Config;
import me.realized.tokenmanager.util.Log;
import me.realized.tokenmanager.util.NumberUtil;
import me.realized.tokenmanager.util.profile.ProfileUtil;
import org.apache.commons.lang.StringEscapeUtils;
import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.command.CommandSender;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.configuration.file.YamlConfiguration;
import org.bukkit.entity.Player;
import org.bukkit.event.player.AsyncPlayerPreLoginEvent;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class MySQLDatabase extends AbstractDatabase {

    private static final long LOGIN_WAIT_DURATION = 30L;
    private static final String SERVER_MODE_MISMATCH = "Server is in %s mode, but found table '%s' does not have column '%s'! Please choose a different table name.";
    private final String table;
    private final ExecutorService executor;
    private final Map<UUID, Double> data = new HashMap<>();

    private HikariDataSource dataSource;

    @Getter
    private JedisPool jedisPool;
    private JedisListener listener;
    private transient boolean usingRedis;

    public MySQLDatabase(final TokenManagerPlugin plugin) {
        super(plugin);
        this.table = StringEscapeUtils.escapeSql(plugin.getConfiguration().getMysqlTable());
        this.executor = Executors.newCachedThreadPool();
        Query.update(table, plugin.getConfiguration().getMysqlColumnId(), plugin.getConfiguration().getMysqlColumnUUID(), plugin.getConfiguration().getMysqlColumnUsername(), plugin.getConfiguration().getMysqlColumnBalance());
    }

    @Override
    public void setup() throws Exception {
        final Config config = plugin.getConfiguration();
        final HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.getMysqlUrl()
            .replace("%hostname%", config.getMysqlHostname())
            .replace("%port%", config.getMysqlPort())
            .replace("%database%", config.getMysqlDatabase())
        );
        hikariConfig.setDriverClassName("com.mysql.jdbc.Driver");
        hikariConfig.setUsername(config.getMysqlUsername());
        hikariConfig.setPassword(config.getMysqlPassword());

        this.dataSource = new HikariDataSource(hikariConfig);

        if (config.isRedisEnabled()) {
            final String password = config.getRedisPassword();

            if (password.isEmpty()) {
                this.jedisPool = new JedisPool(new JedisPoolConfig(), config.getRedisServer(), config.getRedisPort(), 0);
            } else {
                this.jedisPool = new JedisPool(new JedisPoolConfig(), config.getRedisServer(), config.getRedisPort(), 0, password);
            }

            plugin.doAsync(() -> {
                usingRedis = true;

                try (Jedis jedis = jedisPool.getResource()) {
                    jedis.subscribe(listener = new JedisListener(), "tokenmanager");
                } catch (Exception ex) {
                    usingRedis = false;
                    Log.error("Failed to connect to the redis server! Player balance synchronization issues may occur when modifying them while offline.");
                    Log.error("Cause of error: " + ex.getMessage());
                }
            });
        }

        try (
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement()
        ) {
            statement.execute(Query.CREATE_TABLE.query);
        }
    }

    @Override
    public OptionalDouble get(final Player player) {
        if(player != null) {
            UUID uniqueId = player.getUniqueId();
            return from(data.get(uniqueId));
        }
        return OptionalDouble.empty();
    }

    @Override
    public void get(final String uuid, final String username, final Consumer<OptionalDouble> onLoad, final Consumer<String> onError, final boolean create) {
        try (Connection connection = dataSource.getConnection()) {
            onLoad.accept(select(connection, uuid, username, create));
        } catch (Exception ex) {
            if (onError != null) {
                onError.accept(ex.getMessage());
            }

            Log.error("Failed to obtain data for " + uuid + ": " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    @Override
    public void set(final Player player, final double value) {
        data.put(player.getUniqueId(), value);
    }

    @Override
    public void set(final String uuid, final String username, final ModifyType type, final double amount, final double balance, final boolean silent, final Runnable onDone, final Consumer<String> onError) {
        plugin.doAsync(() -> {
            try (Connection connection = dataSource.getConnection()) {
                update(connection, uuid, balance);

                if (usingRedis) {
                    publish(uuid + ":" + type.name() + ":" + amount + ":" + silent);
                } else {
                    plugin.doSync(() -> onModification(uuid, type, amount, silent));
                }

                if (onDone != null) {
                    onDone.run();
                }
            } catch (Exception ex) {
                if (onError != null) {
                    onError.accept(ex.getMessage());
                }

                Log.error("Failed to save data for " + uuid + ": " + ex.getMessage());
                ex.printStackTrace();
            }
        });
    }

    @Override
    public void load(final AsyncPlayerPreLoginEvent event, final Function<Double, Double> modifyLoad) {
        load(event.getUniqueId(), event.getName(), modifyLoad);
    }

    @Override
    public void load(final Player player) {
        load(player.getUniqueId(), player.getName(), null);
    }

    private void load(final UUID uuid, final String username, final Function<Double, Double> modifyLoad) {
        plugin.doAsyncLater(() -> get(uuid.toString(), username, balance -> {
            if (!balance.isPresent()) {
                return;
            }

            plugin.doSync(() -> {
                // Cancel caching if player has left before loading was completed
                if (Bukkit.getPlayer(uuid) == null) {
                    return;
                }

                double totalBalance = balance.getAsDouble();

                if (modifyLoad != null) {
                    totalBalance = modifyLoad.apply(totalBalance);
                }

                data.put(uuid, totalBalance);
            });
        }, null, true), LOGIN_WAIT_DURATION);
    }

    @Override
    public void save(final Player player) {
        final OptionalDouble balance = from(data.remove(player.getUniqueId()));

        if (!balance.isPresent()) {
            return;
        }

        executor.execute(() -> {
            try (Connection connection = dataSource.getConnection()) {
                update(connection, from(player), balance.getAsDouble());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
    }

    @Override
    public void shutdown() throws Exception {
        executor.shutdown();

        if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
            Log.error("Some tasks have failed to execute!");
        }

        try (Connection connection = dataSource.getConnection()) {
            insertCache(connection, data, true);
        } finally {
            for (final AutoCloseable closeable : Arrays.asList(dataSource, listener, jedisPool)) {
                if (closeable != null) {
                    try {
                        closeable.close();
                    } catch (Exception ex) {
                        Log.error("Failed to close " + closeable.getClass().getSimpleName() + ": " + ex.getMessage());
                        ex.printStackTrace();
                    }
                }
            }
        }
    }

    @Override
    public void ordered(final int limit, final Consumer<List<TopElement>> onLoad) {
        final List<TopElement> result = new ArrayList<>();

        if (limit <= 0) {
            onLoad.accept(result);
            return;
        }

        // Create a copy of the current cache to prevent HashMap being accessed by multiple threads
        final Map<UUID, Double> copy = new HashMap<>(data);

        plugin.doAsync(() -> {
            try (Connection connection = dataSource.getConnection()) {
                insertCache(connection, copy, false);
                connection.setAutoCommit(true);

                try (PreparedStatement statement = connection.prepareStatement(Query.SELECT_WITH_LIMIT.query)) {
                    statement.setInt(1, limit);

                    try (ResultSet resultSet = statement.executeQuery()) {
                        while (resultSet.next()) {
                            String uuidString = resultSet.getString(1);
                            String usernameString = resultSet.getString(2);
                            Double balanceDouble = resultSet.getDouble(3);
                            if(uuidString != null && usernameString != null && balanceDouble != null) {
                                result.add(new TopElement(uuidString, usernameString, balanceDouble));
                            }
                        }

                        replaceNames(result, onLoad);
                    }
                }
            } catch (Exception ex) {
                Log.error("Failed to load top balances: " + ex.getMessage());
                ex.printStackTrace();
            }
        });
    }

    @Override
    public void transfer(final CommandSender sender, final Consumer<String> onError) {
        plugin.doAsync(() -> {
            final File file = new File(plugin.getDataFolder(), "data.yml");

            if (!file.exists()) {
                sender.sendMessage(ChatColor.RED + "File not found!");
                return;
            }

            sender.sendMessage(ChatColor.BLUE + plugin.getDescription().getFullName() + ": Loading user data from " + file.getName() + "...");

            final FileConfiguration config = YamlConfiguration.loadConfiguration(file);
            final ConfigurationSection section = config.getConfigurationSection("Players");

            if (section == null) {
                sender.sendMessage(ChatColor.RED + "Data not found!");
                return;
            }

            sender.sendMessage(ChatColor.BLUE + plugin.getDescription().getFullName() + ": Load Complete. Starting the transfer...");

            try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(Query.INSERT_OR_UPDATE.query)
            ) {
                connection.setAutoCommit(false);
                int i = 0;
                final Set<String> keys = section.getKeys(false);

                for (final String key : keys) {
                    final double value = section.getDouble(key);
                    statement.setString(1, key);
                    Player player = Bukkit.getPlayer(UUID.fromString(key));
                    String name = key;
                    if(player != null) {
                        name = player.getName();
                    }
                    statement.setString(2, name);
                    statement.setDouble(3, value);
                    statement.setDouble(4, value);
                    statement.addBatch();

                    if (++i % 100 == 0 || i == keys.size()) {
                        statement.executeBatch();
                    }
                }

                connection.commit();
                connection.setAutoCommit(true);
                sender.sendMessage(ChatColor.BLUE + plugin.getDescription().getFullName() + ": Transfer Complete. Total Transferred Data: " + keys.size());
            } catch (SQLException ex) {
                onError.accept(ex.getMessage());
                Log.error("Failed to transfer data from file: " + ex.getMessage());
                ex.printStackTrace();
            }
        });
    }

    private void insertCache(final Connection connection, final Map<UUID, Double> cache, final boolean remove) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(Query.UPDATE.query)) {
            connection.setAutoCommit(false);

            int i = 0;
            final Collection<? extends Player> players = Bukkit.getOnlinePlayers();

            for (final Player player : players) {
                final Optional<Double> balance = Optional.ofNullable(remove ? cache.remove(player.getUniqueId()) : cache.get(player.getUniqueId()));

                if (!balance.isPresent()) {
                    continue;
                }

                statement.setDouble(1, balance.get());
                statement.setString(2, player.getUniqueId().toString());
                statement.addBatch();

                if (++i % 100 == 0 || i == players.size()) {
                    statement.executeBatch();
                }
            }
        } finally {
            connection.commit();
        }
    }

    private OptionalDouble select(final Connection connection, final String uuid, final String username, final boolean create) throws Exception {
        try (PreparedStatement selectStatement = connection.prepareStatement(Query.SELECT_ONE.query)) {
            selectStatement.setString(1, uuid);

            try (ResultSet resultSet = selectStatement.executeQuery()) {
                if (!resultSet.next()) {
                    if (create) {
                        final double defaultBalance = plugin.getConfiguration().getDefaultBalance();

                        try (PreparedStatement insertStatement = connection.prepareStatement(Query.INSERT.query)) {
                            insertStatement.setString(1, uuid);
                            insertStatement.setString(2, username);
                            insertStatement.setDouble(3, plugin.getConfiguration().getDefaultBalance());
                            insertStatement.execute();
                        }

                        return OptionalDouble.of(defaultBalance);
                    }

                    return OptionalDouble.empty();
                }

                return OptionalDouble.of(resultSet.getDouble(1));
            }
        }
    }

    private void update(final Connection connection, final String key, final double value) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(Query.UPDATE.query)) {
            statement.setDouble(1, value);
            statement.setString(2, key);
            statement.execute();
        }
    }

    private void onModification(final String key, final ModifyType type, final double amount, final boolean silent) {
        final Player player;

        if (ProfileUtil.isUUID(key)) {
            player = Bukkit.getPlayer(UUID.fromString(key));
        } else {
            player = Bukkit.getPlayerExact(key);
        }

        if (player == null) {
            return;
        }

        if (type == ModifyType.SET) {
            set(player, amount);
            return;
        }

        final OptionalDouble cached;

        if (!(cached = get(player)).isPresent()) {
            return;
        }

        set(player, type.apply(cached.getAsDouble(), amount));

        if (silent) {
            return;
        }

        plugin.getLang().sendMessage(player, true, "COMMAND." + (type == ModifyType.ADD ? "add" : "remove"), "amount", amount);
    }

    private void publish(final String message) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.publish("tokenmanager", message);
        } catch (JedisConnectionException ignored) {}
    }

    private enum Query {

        CREATE_TABLE("CREATE TABLE IF NOT EXISTS {table} ({column_id} INT AUTO_INCREMENT PRIMARY KEY, {column_uuid} VARCHAR(36) NOT NULL UNIQUE, {column_username} VARCHAR(16) NOT NULL UNIQUE, {column_balance} DOUBLE(11,2) NOT NULL);"),
        SELECT_WITH_LIMIT("SELECT {column_uuid}, {column_username}, {column_balance} FROM {table} ORDER BY {column_balance} DESC LIMIT ?;"),
        SELECT_ONE("SELECT {column_balance} FROM {table} WHERE {column_uuid}=?;"),
        INSERT("INSERT INTO {table} ({column_uuid}, {column_username}, {column_balance}) VALUES (?, ?, ?);"),
        UPDATE("UPDATE {table} SET {column_balance}=? WHERE {column_uuid}=?;"),
        INSERT_OR_UPDATE("INSERT INTO {table} ({column_uuid}, {column_username}, {column_balance}) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE {column_balance}=?;");

        private String query;

        Query(final String query) {
            this.query = query;
        }

        private static void update(final String table, final String columnIdName, final String columnUUIDName, final String columnUsernameName, final String columnBalanceName) {
            for (final Query query : values()) {
                query.replace(s -> s.replace("{table}", table)
                        .replace("{column_id}", columnIdName)
                        .replace("{column_uuid}", columnUUIDName)
                        .replace("{column_username}", columnUsernameName)
                        .replace("{column_balance}", columnBalanceName)
                );

            }
        }

        private void replace(final Function<String, String> function) {
            this.query = function.apply(query);
        }
    }

    private class JedisListener extends JedisPubSub implements AutoCloseable {

        @Override
        public void onMessage(final String channel, final String message) {
            final String[] args = message.split(":");

            if (args.length < 3) {
                return;
            }

            plugin.doSync(() -> {
                final ModifyType type = ModifyType.valueOf(args[1]);
                final OptionalDouble amount = NumberUtil.parseLong(args[2]);

                if (!amount.isPresent()) {
                    return;
                }

                onModification(args[0], type, amount.getAsDouble(), args[3].equals("true"));
            });
        }

        @Override
        public void close() {
            unsubscribe();
        }
    }
}
