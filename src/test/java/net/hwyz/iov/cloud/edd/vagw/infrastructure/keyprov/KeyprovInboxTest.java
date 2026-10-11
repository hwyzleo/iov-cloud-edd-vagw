package net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov;

import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * keyprov 传输层短期去重测试（EDD-VAGW-DSN-CR-011 §3.4/§7）。
 */
class KeyprovInboxTest {

    private final KeyprovProperties properties = new KeyprovProperties();
    private final KeyprovInbox inbox = new KeyprovInbox(properties);

    @Test
    void record_newThenSameDigest_shouldBeDuplicate() {
        assertEquals(KeyprovInbox.State.NEW, inbox.record("m1", "d1"));
        assertEquals(KeyprovInbox.State.DUPLICATE, inbox.record("m1", "d1"));
    }

    @Test
    void record_sameMessageIdDifferentDigest_shouldBeConflict() {
        assertEquals(KeyprovInbox.State.NEW, inbox.record("m1", "d1"));
        assertEquals(KeyprovInbox.State.CONFLICT, inbox.record("m1", "d2"));
    }

    @Test
    void record_blankMessageId_shouldAlwaysBeNew() {
        assertEquals(KeyprovInbox.State.NEW, inbox.record("", "d1"));
        assertEquals(KeyprovInbox.State.NEW, inbox.record("", "d1"));
    }

    @Test
    void forget_shouldAllowReRecord() {
        inbox.record("m1", "d1");
        inbox.forget("m1");
        assertEquals(KeyprovInbox.State.NEW, inbox.record("m1", "d1"));
    }
}
