package vessel

import (
	"errors"
	"strings"
)

type Segment struct {
	ID               string `json:"id"`
	From             string `json:"from"`
	To               string `json:"to"`
	RequiresCleaning bool   `json:"requires_cleaning"`
	Open             bool   `json:"open"`
}

type Route struct {
	ID       string    `json:"id"`
	Owner    string    `json:"owner"`
	Segments []Segment `json:"segments"`
}

func NewRoute(id, owner string, segments []Segment) (Route, error) {
	if strings.TrimSpace(id) == "" || strings.TrimSpace(owner) == "" {
		return Route{}, errors.New("route id and owner are required")
	}
	if len(segments) == 0 {
		return Route{}, errors.New("route needs at least one segment")
	}
	return Route{ID: id, Owner: owner, Segments: append([]Segment(nil), segments...)}, nil
}

func (r Route) Clone() Route {
	return r
}

func (r *Route) MarkCleaning(blocked map[string]bool) {
	for index := range r.Segments {
		r.Segments[index].RequiresCleaning = blocked[r.Segments[index].ID]
	}
}

func (r Route) Available() bool {
	if len(r.Segments) == 0 {
		return false
	}
	for _, segment := range r.Segments {
		if segment.RequiresCleaning {
			return false
		}
	}
	return true
}
