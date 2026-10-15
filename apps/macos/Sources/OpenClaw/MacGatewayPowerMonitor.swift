import AppKit
import Foundation
import IOKit
import IOKit.pwr_mgt

@MainActor
final class MacGatewayPowerMonitor {
    static let shared = MacGatewayPowerMonitor()

    private let capabilities: @MainActor () -> UInt32?
    private let clock: any Clock<Duration>
    private var sleeping = false
    private var observers: [NSObjectProtocol] = []
    private var wakeTask: Task<Void, Never>?

    init(
        capabilities: @escaping @MainActor () -> UInt32? = MacGatewayPowerMonitor.systemCapabilities,
        clock: any Clock<Duration> = ContinuousClock())
    {
        self.capabilities = capabilities
        self.clock = clock
    }

    var allowsGatewayLaunch: Bool {
        guard !self.sleeping else { return false }
        guard let capabilities = self.capabilities() else { return true }
        // Graphics is a system capability, not whether a display is currently lit.
        // Unknown/unsupported registry values retain the existing startup behavior.
        return capabilities & UInt32(kIOPMSystemCapabilityCPU) == 0 ||
            capabilities & UInt32(kIOPMSystemCapabilityGraphics) != 0
    }

    func start(
        deferWake: @escaping @MainActor () -> Bool,
        onSleep: @escaping @MainActor () -> Void,
        onWake: @escaping @MainActor () -> Void)
    {
        guard self.observers.isEmpty else { return }
        let center = NSWorkspace.shared.notificationCenter
        self.observers = [
            center.addObserver(forName: NSWorkspace.willSleepNotification, object: nil, queue: .main) { [weak self] _ in
                Task { @MainActor in
                    self?.willSleep()
                    onSleep()
                }
            },
            center.addObserver(forName: NSWorkspace.didWakeNotification, object: nil, queue: .main) { [weak self] _ in
                Task { @MainActor in
                    guard let self else { return }
                    self.wakeTask?.cancel()
                    self.wakeTask = Task { await self.didWake(waitForFullWake: deferWake(), onWake: onWake) }
                }
            },
        ]
    }

    func willSleep() {
        self.sleeping = true
        self.wakeTask?.cancel()
    }

    func didWake(waitForFullWake: Bool = true, onWake: @MainActor () -> Void) async {
        self.sleeping = false
        if !waitForFullWake {
            onWake()
            return
        }
        // RootDomain sends its wake message before publishing System Capabilities.
        // Bound these rechecks to this notification; DarkWake never starts a polling loop.
        for delay in [Duration.zero, .milliseconds(250), .milliseconds(500), .seconds(1)] {
            do {
                if delay > .zero { try await self.clock.sleep(for: delay) }
                try Task.checkCancellation()
            } catch { return }
            guard !self.sleeping else { return }
            if self.allowsGatewayLaunch {
                onWake()
                return
            }
        }
    }

    private static func systemCapabilities() -> UInt32? {
        let service = IOServiceGetMatchingService(kIOMainPortDefault, IOServiceMatching("IOPMrootDomain"))
        guard service != 0 else { return nil }
        defer { IOObjectRelease(service) }
        // Public IORegistry access; this property is an undocumented macOS contract.
        return (IORegistryEntryCreateCFProperty(service, "System Capabilities" as CFString, nil, 0)?
            .takeRetainedValue() as? NSNumber)?.uint32Value
    }
}
