package gaming

import "testing"

// A frame has to be recognised whatever game it belongs to, and whatever
// version it carries: an installation that has never heard of a game must still
// know to keep its traffic out of the chat surface.
func TestIsEnvelopeRecognisesFrames(t *testing.T) {
	frames := []string{
		SampleEnvelope,
		`--gaming[v=1,game=poker,gv=1,sid=ab,mid=cd,seq=1/1,exp=0]--QUJD`,
		`--gaming[v=9,game=chess,gv=7,sid=ab,mid=cd,seq=2/4,exp=0]--QUJD`,
		`--gaming[]--QUJD`,
		"  " + SampleEnvelope + "\n",
	}
	for _, f := range frames {
		if !IsEnvelope(f) {
			t.Errorf("frame not recognised: %q", f)
		}
	}
}

// The match is anchored over the whole body, so somebody discussing the
// protocol in chat does not have their message silently disappear.
func TestIsEnvelopeLeavesChatAlone(t *testing.T) {
	chat := []string{
		"",
		"hello",
		"have a look at --gaming[v=1,game=poker]--QUJD and tell me what you think",
		// Frame-shaped, but the payload is prose rather than base64.
		"--gaming[v=1,game=poker,gv=1,sid=ab,mid=cd,seq=1/1,exp=0]--QUJD trailing words",
		"prefix --gaming[v=1]--QUJD",
		// An empty payload carries nothing and is not a frame.
		"--gaming[v=1,game=poker]--",
		"--mcp[v=1,sid=ab,mid=cd,seq=1/1,exp=0]--QUJD",
		"--embed[type=image/png,data=AAAA]--",
		// The payload alphabet excludes brackets, so a body cannot carry a
		// second tag past the first.
		`--gaming[v=1,game=poker]--QUJD--gaming[v=1,game=poker]--QUJD`,
	}
	for _, c := range chat {
		if IsEnvelope(c) {
			t.Errorf("ordinary message swallowed as a frame: %q", c)
		}
	}
}

// The sample exists so hosts can test content-filter rules against it. If it
// stopped being a frame the guard built on it would silently pass everything.
func TestSampleEnvelopeIsAFrame(t *testing.T) {
	if !IsEnvelope(SampleEnvelope) {
		t.Fatal("SampleEnvelope must itself be recognised, or filter guards are inert")
	}
}
