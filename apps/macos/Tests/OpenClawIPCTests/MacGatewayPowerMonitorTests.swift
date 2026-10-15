import Testing
@testable import OpenClaw

@MainActor
struct MacGatewayPowerMonitorTests {
    @Test(arguments: [nil, UInt32(0), 1, 3, 5, 13, 15])
    func `only CPU without graphics defers startup`(capabilities: UInt32?) {
        let monitor = MacGatewayPowerMonitor(capabilities: { capabilities })
        #expect(monitor.allowsGatewayLaunch == ![UInt32(1), 5, 13].contains(capabilities ?? 0))
        monitor.willSleep()
        #expect(!monitor.allowsGatewayLaunch)
    }

    @Test func `service hosting retains immediate wake handling during DarkWake`() async {
        let monitor = MacGatewayPowerMonitor(capabilities: { 13 })
        monitor.willSleep()
        var wakeCount = 0
        await monitor.didWake(waitForFullWake: false) { wakeCount += 1 }
        #expect(wakeCount == 1)
        #expect(!monitor.allowsGatewayLaunch)
    }

    @Test(arguments: [false, true])
    func `wake waits for registry publication and abandons a new sleep`(sleepAgain: Bool) async {
        let capabilities = GatewayPowerTestCapabilities(1)
        let clock = ManualTestClock()
        let monitor = MacGatewayPowerMonitor(capabilities: { capabilities.value }, clock: clock)
        monitor.willSleep()
        var wakeCount = 0
        let wake = Task { await monitor.didWake { wakeCount += 1 } }
        await clock.waitForSleep(until: clock.now + .milliseconds(250))
        capabilities.value = 3
        if sleepAgain { monitor.willSleep() }
        clock.advance(by: .milliseconds(250))
        await wake.value
        #expect(wakeCount == (sleepAgain ? 0 : 1))
        #expect(monitor.allowsGatewayLaunch == !sleepAgain)
    }

    @Test func `DarkWake rechecks end without resuming the gateway`() async {
        let clock = ManualTestClock()
        let monitor = MacGatewayPowerMonitor(capabilities: { 1 }, clock: clock)
        var wakeCount = 0
        let wake = Task { await monitor.didWake { wakeCount += 1 } }
        for delay in [Duration.milliseconds(250), .milliseconds(500), .seconds(1)] {
            await clock.waitForSleep(until: clock.now + delay)
            clock.advance(by: delay)
        }
        await wake.value
        #expect(wakeCount == 0)
        #expect(!monitor.allowsGatewayLaunch)
    }
}

@MainActor
final class GatewayPowerTestCapabilities {
    var value: UInt32?

    init(_ value: UInt32?) {
        self.value = value
    }
}
