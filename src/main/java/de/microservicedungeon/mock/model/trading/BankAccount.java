package de.microservicedungeon.mock.model.trading;

import java.util.UUID;

public record BankAccount(
        UUID bankAccountId,
        int balance
) {
}
