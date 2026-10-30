package ax

import (
	"os"
	"testing"
)

// Every AX variable that production reads from the environment, except AX_HOME.
// A shell inside an AX-launched session carries them: fixtures would be rejected
// as an earlier launch, spawnCLI would adopt the live session as its parent, and
// launches and attachments would be opted in or given a delivery boundary. Tests
// set what they need with t.Setenv. AX_HOME stays: only Main reads it, in the
// child that TestAttachedCLIExitsWhileStdinIsIdle re-executes with the fixture's
// home, and TestMain runs in that child too.
func TestMain(m *testing.M) {
	for _, name := range []string{"AX_LAUNCH_ID", "AX_SESSION_FILE", "AX_ALLOW_BYPASS", "AX_EXTERNAL", "AX_DELIVERY_BOUNDARY"} {
		os.Unsetenv(name)
	}
	os.Exit(m.Run())
}
