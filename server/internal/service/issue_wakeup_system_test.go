package service

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

func sub(stage int32, closed, cancelled bool) subIssue {
	// Closed implies Delivered, mirroring loadSubIssues.
	s := subIssue{Closed: closed, Cancelled: cancelled, Delivered: closed}
	if stage > 0 {
		s.Stage = pgtype.Int4{Int32: stage, Valid: true}
	}
	return s
}

// The system rule's reading: a stage closing while a later one waits, then
// every sub-issue closing. People's children_done conditions keep theirs.
func TestStageProgress(t *testing.T) {
	cases := []struct {
		name        string
		children    []subIssue
		met         bool
		fingerprint string
		next        int32
	}{
		{"no sub-issues", nil, false, "", 0},
		{"unstaged, one open", []subIssue{sub(0, true, false), sub(0, false, false)}, false, "", 0},
		{"unstaged, all closed", []subIssue{sub(0, true, false), sub(0, true, true)}, true, "all", 0},
		{"stage 1 closed, stage 2 waits", []subIssue{sub(1, true, false), sub(1, true, true), sub(2, false, false)}, true, "stage:1", 2},
		{"two stages closed, stage 3 waits", []subIssue{sub(1, true, false), sub(2, true, false), sub(3, false, false)}, true, "stage:2", 3},
		{"a later stage closed first", []subIssue{sub(1, false, false), sub(2, true, false)}, false, "", 0},
		{"last stage closed, unstaged open", []subIssue{sub(1, true, false), sub(2, true, false), sub(0, false, false)}, false, "", 0},
		{"everything closed", []subIssue{sub(1, true, false), sub(2, true, true), sub(0, true, false)}, true, "all", 0},
		{"stage waits on unstaged-free set", []subIssue{sub(1, false, false), sub(0, true, false)}, false, "", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			met, fingerprint, observed := stageProgress(tc.children)
			if met != tc.met || fingerprint != tc.fingerprint {
				t.Fatalf("stageProgress = %v %q, want %v %q (%v)", met, fingerprint, tc.met, tc.fingerprint, observed)
			}
			if tc.next != 0 && observed["next_stage"] != tc.next {
				t.Fatalf("next_stage = %v, want %d", observed["next_stage"], tc.next)
			}
		})
	}
}

func TestStageProgressCountsCancellations(t *testing.T) {
	_, _, observed := stageProgress([]subIssue{sub(1, true, true), sub(1, true, false), sub(2, false, false)})
	stages := observed["stages"].([]stageCount)
	if observed["cancelled"] != 1 || len(stages) != 2 || stages[0] != (stageCount{Stage: 1, Total: 2, Closed: 2, Cancelled: 1, Delivered: 2}) {
		t.Fatalf("observed = %+v", observed)
	}
}

func TestChildrenDoneStageCondition(t *testing.T) {
	stage := int32(1)
	children := []subIssue{sub(1, true, false), sub(2, false, false), sub(0, false, false)}
	if met, _, _ := childrenDone(WakeupCondition{Type: "children_done", Stage: &stage}, children); !met {
		t.Fatal("stage 1 closed but the stage condition did not hold")
	}
	if met, _, _ := childrenDone(WakeupCondition{Type: "children_done"}, children); met {
		t.Fatal("the all-children condition held while sub-issues were open")
	}
	missing := int32(3)
	if met, _, _ := childrenDone(WakeupCondition{Type: "children_done", Stage: &missing}, children); met {
		t.Fatal("a stage without sub-issues held")
	}
}

func TestChildDoneInstructionPrecedence(t *testing.T) {
	settings := []byte(`{"system_wakeup_child_done":false,"system_wakeup_child_done_instruction":"  Workspace way  "}`)
	if enabled, instruction := SystemWakeupDefault(settings); enabled || instruction != "Workspace way" {
		t.Fatalf("default = %v %q", enabled, instruction)
	}
	if got := ChildDoneInstruction(" Issue way ", settings); got != "Issue way" {
		t.Fatalf("issue instruction = %q", got)
	}
	if got := ChildDoneInstruction("", settings); got != "Workspace way" {
		t.Fatalf("workspace instruction = %q", got)
	}
	if got := ChildDoneInstruction("", []byte(`{}`)); got != ChildDoneDefaultInstruction {
		t.Fatalf("built-in instruction = %q", got)
	}
	if enabled, _ := SystemWakeupDefault([]byte(`not json`)); !enabled {
		t.Fatal("unreadable settings must keep the rule on")
	}
}

// subD builds a sub-issue with an explicit delivered flag (the delivered tier
// of the child_done system rule).
func subD(stage int32, closed, cancelled, delivered bool) subIssue {
	s := sub(stage, closed, cancelled)
	s.Delivered = delivered
	return s
}

func TestStageProgressDeliveredTier(t *testing.T) {
	cases := []struct {
		name        string
		children    []subIssue
		met         bool
		fingerprint string
		stage       int32
	}{
		{"stage 1 delivered, not closed", []subIssue{subD(1, false, false, true), subD(2, false, false, false)}, true, "delivered:1", 1},
		{"delivered mixes in_review and done", []subIssue{subD(1, false, false, true), sub(1, true, false), subD(2, false, false, false)}, true, "delivered:1", 1},
		{"closed frontier outranks delivered", []subIssue{sub(1, true, false), subD(2, false, false, true)}, true, "stage:1", 0},
		{"all closed outranks delivered", []subIssue{sub(1, true, false), sub(1, true, false)}, true, "all", 0},
		{"cancelled sibling counts delivered", []subIssue{sub(1, true, true), subD(1, false, false, true)}, true, "delivered:1", 1},
		{"in progress is not delivered", []subIssue{subD(1, false, false, false), subD(2, false, false, false)}, false, "", 0},
		{"last stage delivered, unstaged open", []subIssue{subD(1, false, false, true)}, true, "delivered:1", 1},
		{"no children", nil, false, "", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			met, fingerprint, observed := stageProgress(tc.children)
			if met != tc.met || fingerprint != tc.fingerprint {
				t.Fatalf("stageProgress = %v %q, want %v %q (%v)", met, fingerprint, tc.met, tc.fingerprint, observed)
			}
			if tc.stage != 0 && observed["delivered_stage"] != tc.stage {
				t.Fatalf("delivered_stage = %v, want %d", observed["delivered_stage"], tc.stage)
			}
		})
	}
}

func TestStageProgressDeliveredFiresAfterClosedNews(t *testing.T) {
	// stage:1 fired first; once it is the rule's state, the delivered
	// fingerprint of the next stage is news and fires in a later pass.
	if !conditionNewlyMet("stage:1", "delivered:2") {
		t.Fatal("delivered:2 after stage:1 must be news")
	}
	if conditionNewlyMet("delivered:2", "delivered:2") {
		t.Fatal("the same delivered stage must not fire twice")
	}
	// Accepting everything moves to the final verdict; it is news again.
	if !conditionNewlyMet("delivered:2", "all") {
		t.Fatal("all after delivered must be news")
	}
}

func TestChildrenDoneUserConditionsIgnoreDelivered(t *testing.T) {
	// The delivered tier belongs to the system rule (EachStage). A user's
	// children_done condition still waits for closed, whatever the stage.
	children := []subIssue{subD(1, false, false, true), subD(0, false, false, true)}
	stage := int32(1)
	if met, _, _ := childrenDone(WakeupCondition{Type: "children_done"}, children); met {
		t.Fatal("all-children condition held while nothing closed")
	}
	if met, _, _ := childrenDone(WakeupCondition{Type: "children_done", Stage: &stage}, children); met {
		t.Fatal("stage condition held while the stage is not closed")
	}
}

func TestChildDoneInstructionMentionsDelivered(t *testing.T) {
	if !strings.Contains(ChildDoneDefaultInstruction, "delivered") || !strings.Contains(ChildDoneDefaultInstruction, "in review") {
		t.Fatalf("default instruction must explain the delivered tier: %q", ChildDoneDefaultInstruction)
	}
}
