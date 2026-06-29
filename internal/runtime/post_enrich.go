// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"encoding/base64"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/companyzero/bisonrelay/rpc"
	"github.com/companyzero/bisonrelay/zkidentity"
)

// postEmbedRE matches BR's --embed[k=v,...]-- post body tag (and the parallel
// --download[...]-- chat-side tag, which posts drop). Mirrors the dashboard's
// brPostEmbedRE so author and consumer agree on the wire form.
var postEmbedRE = regexp.MustCompile(`--(embed|download)\[(.*?)\]--`)

const (
	// feedTitleCap matches clientdb's maxTitleLen.
	feedTitleCap   = 255
	feedSnippetCap = 280
)

// feedEmbed describes one --embed[...]-- tag without its inline payload;
// bytes are served separately by /posts/embed-data.
type feedEmbed struct {
	Index    int    `json:"index"`
	Mime     string `json:"mime"`
	Alt      string `json:"alt,omitempty"`
	Filename string `json:"filename,omitempty"`
	Size     int    `json:"size,omitempty"`
	Cost     uint64 `json:"cost,omitempty"`
	Download string `json:"download,omitempty"`
	HasData  bool   `json:"has_data"`
}

type feedFirstImage struct {
	Index      int    `json:"index"`
	Mime       string `json:"mime"`
	Alt        string `json:"alt,omitempty"`
	HasData    bool   `json:"has_data"`
	IsDownload bool   `json:"is_download"`
}

type feedHeart struct {
	User string `json:"user"`
	Nick string `json:"nick"`
}

type feedPost struct {
	ID              string          `json:"id"`
	From            string          `json:"from"`
	AuthorID        string          `json:"author_id"`
	AuthorNick      string          `json:"author_nick"`
	Date            int64           `json:"date"`
	Published       int64           `json:"published,omitempty"`
	LastStatusTS    int64           `json:"last_status_ts"`
	Title           string          `json:"title"`
	Description     string          `json:"description,omitempty"`
	Snippet         string          `json:"snippet,omitempty"`
	HasMore         bool            `json:"has_more,omitempty"`
	Relayed         bool            `json:"relayed,omitempty"`
	RelayerNick     string          `json:"relayer_nick,omitempty"`
	HeartsCount     int             `json:"hearts_count"`
	HeartedByMe     bool            `json:"hearted_by_me"`
	HeartedBy       []feedHeart     `json:"hearted_by,omitempty"`
	CommentsCount   int             `json:"comments_count"`
	CommenterCount  int             `json:"commenter_count"`
	LastCommentTS   int64           `json:"last_comment_ts,omitempty"`
	LastCommentNick string          `json:"last_comment_nick,omitempty"`
	ReceiptCount    int             `json:"receipt_count,omitempty"`
	Embeds          []feedEmbed     `json:"embeds,omitempty"`
	FirstImage      *feedFirstImage `json:"first_image,omitempty"`
}

// parseEmbedArgs parses the comma-separated k=v list inside an --embed[...]--
// tag. data= payloads are flagged but not retained; /posts/embed-data
// re-extracts the bytes on demand. "part" is accepted as an alias for "name"
// because a relayed embed arrives re-serialized with part= (mdembeds
// EmbeddedArgs.String) where the author wrote name=.
func parseEmbedArgs(inner string) feedEmbed {
	var e feedEmbed
	for _, part := range strings.Split(inner, ",") {
		eq := strings.Index(part, "=")
		if eq < 0 {
			continue
		}
		k, v := part[:eq], part[eq+1:]
		switch k {
		case "name", "part":
			if e.Filename == "" {
				e.Filename = v
			}
		case "filename":
			e.Filename = v
		case "type":
			e.Mime = v
		case "data":
			if v != "" {
				e.HasData = true
			}
		case "size":
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				e.Size = n
			}
		case "alt":
			if dec, err := url.QueryUnescape(v); err == nil {
				e.Alt = dec
			} else {
				e.Alt = v
			}
		case "download":
			e.Download = v
		case "cost":
			if n, err := strconv.ParseUint(v, 10, 64); err == nil {
				e.Cost = n
			}
		}
	}
	return e
}

// stripEmbeds removes every embed tag from a post body, returning the plain
// text between tags and the parsed embed list. Chat-side --download[...]--
// tags are dropped without a feed entry, matching the dashboard renderer.
func stripEmbeds(main string) (string, []feedEmbed) {
	if main == "" {
		return "", nil
	}
	matches := postEmbedRE.FindAllStringSubmatchIndex(main, -1)
	if len(matches) == 0 {
		return main, nil
	}
	var b strings.Builder
	var embeds []feedEmbed
	last := 0
	for _, m := range matches {
		b.WriteString(main[last:m[0]])
		if main[m[2]:m[3]] == "embed" {
			e := parseEmbedArgs(main[m[4]:m[5]])
			e.Index = len(embeds)
			embeds = append(embeds, e)
		}
		last = m[1]
	}
	b.WriteString(main[last:])
	return b.String(), embeds
}

// embedDataAt returns the mime and inline base64 payload of the idx-th
// --embed[...]-- tag in a post body. Walked on demand so the feed path never
// holds payload bytes.
func embedDataAt(main string, idx int) (mime, dataB64 string, ok bool) {
	n := 0
	for _, m := range postEmbedRE.FindAllStringSubmatchIndex(main, -1) {
		if main[m[2]:m[3]] != "embed" {
			continue
		}
		if n != idx {
			n++
			continue
		}
		for _, part := range strings.Split(main[m[4]:m[5]], ",") {
			eq := strings.Index(part, "=")
			if eq < 0 {
				continue
			}
			switch part[:eq] {
			case "type":
				mime = part[eq+1:]
			case "data":
				dataB64 = part[eq+1:]
			}
		}
		return mime, dataB64, dataB64 != ""
	}
	return "", "", false
}

func truncateRunes(s string, max int) string {
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	return string(runes[:max])
}

// deriveTitle picks the feed title: the explicit title attribute when set,
// else the first non-empty line of the embed-stripped body. Mirrors BR's
// clientintf.PostTitle except shortcodes are removed first so a leading
// image embed cannot leak into the title.
func deriveTitle(attrs map[string]string, plain string) string {
	src := strings.TrimSpace(attrs[rpc.RMPTitle])
	if src == "" {
		src = plain
	}
	src = strings.ReplaceAll(src, "\r", "\n")
	for _, line := range strings.Split(src, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		return truncateRunes(line, feedTitleCap)
	}
	return ""
}

// fallbackEmbedTitle names a post whose body is only embeds.
func fallbackEmbedTitle(embeds []feedEmbed) string {
	for _, e := range embeds {
		if !strings.HasPrefix(e.Mime, "image/") {
			continue
		}
		if e.Alt != "" {
			return truncateRunes(e.Alt, feedTitleCap)
		}
		if e.Filename != "" {
			return e.Filename
		}
		return "(image)"
	}
	if len(embeds) > 0 {
		e := embeds[0]
		if e.Alt != "" {
			return truncateRunes(e.Alt, feedTitleCap)
		}
		if e.Filename != "" {
			return e.Filename
		}
		return "(attachment)"
	}
	return ""
}

// deriveSnippet collapses the embed-stripped body into a single-spaced
// preview capped at max runes. The bool reports whether content was cut.
func deriveSnippet(plain string, max int) (string, bool) {
	s := strings.Join(strings.Fields(plain), " ")
	if s == "" {
		return "", false
	}
	runes := []rune(s)
	if len(runes) <= max {
		return s, false
	}
	return strings.TrimSpace(string(runes[:max])) + "...", true
}

// firstImage picks the first image embed for the feed card hero.
func firstImage(embeds []feedEmbed) *feedFirstImage {
	for _, e := range embeds {
		if !strings.HasPrefix(e.Mime, "image/") {
			continue
		}
		return &feedFirstImage{
			Index:      e.Index,
			Mime:       e.Mime,
			Alt:        e.Alt,
			HasData:    e.HasData,
			IsDownload: e.Download != "",
		}
	}
	return nil
}

// aggregateHearts folds time-ordered status updates into the current heart
// state: the last RMPSHeart per user is their toggle state (1 adds, 0
// removes; rpc/routedrpc.go). Same walk as handlePostHearts.
func aggregateHearts(updates []rpc.PostMetadataStatus, myID string) (count int, byMe bool, by []feedHeart) {
	type heartState struct {
		mode string
		nick string
	}
	last := make(map[string]heartState)
	order := make([]string, 0, len(updates))
	for _, u := range updates {
		if u.Attributes == nil {
			continue
		}
		mode, ok := u.Attributes[rpc.RMPSHeart]
		if !ok || (mode != rpc.RMPSHeartYes && mode != rpc.RMPSHeartNo) {
			continue
		}
		from := u.Attributes[rpc.RMPStatusFrom]
		if _, seen := last[from]; !seen {
			order = append(order, from)
		}
		last[from] = heartState{mode: mode, nick: u.Attributes[rpc.RMPFromNick]}
	}
	for _, from := range order {
		st := last[from]
		if st.mode != rpc.RMPSHeartYes {
			continue
		}
		if from == myID {
			byMe = true
		}
		by = append(by, feedHeart{User: from, Nick: st.nick})
	}
	return len(by), byMe, by
}

// aggregateComments counts comment status updates, distinct commenters and
// the newest comment, then merges sent-but-unreplicated own comments using
// the same text+parent dedupe as /posts/comments so feed counts match the
// detail thread.
func aggregateComments(updates []rpc.PostMetadataStatus, myID, myNick string,
	unrepl []unreplComment) (count, commenters int, lastTS int64, lastNick string) {

	seen := make(map[string]struct{})
	ownKeys := make(map[string]struct{})
	for _, u := range updates {
		if u.Attributes == nil {
			continue
		}
		body := u.Attributes[rpc.RMPSComment]
		if body == "" {
			continue
		}
		from := u.Attributes[rpc.RMPStatusFrom]
		count++
		seen[from] = struct{}{}
		if from == myID {
			ownKeys[body+"\x00"+u.Attributes[rpc.RMPParent]] = struct{}{}
		}
		var ts int64
		if tsStr := u.Attributes[rpc.RMPTimestamp]; tsStr != "" {
			// Status updates carry hex timestamps (BR client_posts.go
			// writes FormatInt(.., 16)); a base-10 parse turns all-digit
			// hex values into dates around 1972.
			if n, err := strconv.ParseInt(tsStr, 16, 64); err == nil {
				ts = n
			}
		}
		if ts >= lastTS {
			lastTS = ts
			lastNick = u.Attributes[rpc.RMPFromNick]
		}
	}
	for _, e := range unrepl {
		if _, dup := ownKeys[e.Comment+"\x00"+e.Parent]; dup {
			continue
		}
		count++
		seen[myID] = struct{}{}
		if e.Timestamp >= lastTS {
			lastTS = e.Timestamp
			lastNick = myNick
		}
	}
	return count, len(seen), lastTS, lastNick
}

// maxEmbedServeBytes bounds a single inline embed served over /posts/embed-data.
// A post body (and thus any one embed) is already capped by BR's max message
// size; this makes the ceiling explicit so a crafted post cannot force an
// outsized allocation or transfer.
const maxEmbedServeBytes = 16 << 20

// handlePostsEmbedData streams the inline payload of one --embed[...]-- tag
// from a post body, selected by ?uid=<author>&pid=<post>&index=<n>. Only
// data= embeds are served; download (paid file transfer) embeds 404 since
// those bytes are fetched via /content/get with explicit consent. Posts are
// content-addressed and immutable, so responses carry a long-lived cache
// header.
func (s *StatusServer) handlePostsEmbedData(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	uidStr := strings.TrimSpace(r.URL.Query().Get("uid"))
	pidStr := strings.TrimSpace(r.URL.Query().Get("pid"))
	if uidStr == "" || pidStr == "" {
		http.Error(w, "uid and pid query params are required", http.StatusBadRequest)
		return
	}
	var uid zkidentity.ShortID
	if err := uid.FromString(uidStr); err != nil {
		http.Error(w, "invalid uid: "+err.Error(), http.StatusBadRequest)
		return
	}
	var pid zkidentity.ShortID
	if err := pid.FromString(pidStr); err != nil {
		http.Error(w, "invalid pid: "+err.Error(), http.StatusBadRequest)
		return
	}
	idx := 0
	if idxStr := strings.TrimSpace(r.URL.Query().Get("index")); idxStr != "" {
		n, err := strconv.Atoi(idxStr)
		if err != nil || n < 0 {
			http.Error(w, "invalid index", http.StatusBadRequest)
			return
		}
		idx = n
	}
	pm, err := c.ReadPost(uid, pid)
	if err != nil {
		http.Error(w, "read post: "+err.Error(), http.StatusNotFound)
		return
	}
	mime, dataB64, ok := embedDataAt(pm.Attributes[rpc.RMPMain], idx)
	if !ok {
		http.Error(w, "no inline data at embed index", http.StatusNotFound)
		return
	}
	raw, err := base64.StdEncoding.DecodeString(dataB64)
	if err != nil {
		http.Error(w, "invalid embed data: "+err.Error(), http.StatusBadRequest)
		return
	}
	if len(raw) > maxEmbedServeBytes {
		http.Error(w, "embed too large", http.StatusRequestEntityTooLarge)
		return
	}
	if mime == "" {
		mime = http.DetectContentType(raw)
	}
	w.Header().Set("Content-Type", mime)
	w.Header().Set("Content-Length", strconv.Itoa(len(raw)))
	w.Header().Set("Cache-Control", "private, max-age=31536000, immutable")
	_, _ = w.Write(raw)
}
