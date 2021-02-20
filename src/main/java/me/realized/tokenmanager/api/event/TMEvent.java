package me.realized.tokenmanager.api.event;

import org.bukkit.entity.Player;
import org.bukkit.event.Cancellable;
import org.bukkit.event.Event;

abstract class TMEvent extends Event implements Cancellable {

    private final Player player;
    private final double amount;
    private boolean cancelled;

    TMEvent(final Player player, final double amount) {
        this.player = player;
        this.amount = amount;
    }

    public Player getPlayer() {
        return player;
    }

    public double getAmount() {
        return amount;
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public void setCancelled(final boolean cancelled) {
        this.cancelled = cancelled;
    }
}
