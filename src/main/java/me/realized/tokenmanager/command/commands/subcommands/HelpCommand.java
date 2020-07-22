package me.realized.tokenmanager.command.commands.subcommands;

import me.realized.tokenmanager.TokenManagerPlugin;
import me.realized.tokenmanager.command.BaseCommand;
import org.bukkit.command.CommandSender;

public class HelpCommand extends BaseCommand {

    public HelpCommand(final TokenManagerPlugin plugin) {
        super(plugin, "help", "help", null, 1, false);
    }

    @Override
    protected void execute(CommandSender sender, String label, String[] args) {
        sendMessage(sender, true, "COMMAND.token.help");
    }

}
