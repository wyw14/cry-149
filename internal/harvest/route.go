package harvest

import (
	"context"
	"sync"
	"time"

	"github.com/wyw14/cry-149/internal/utility"
	"github.com/wyw14/cry-149/internal/vessel"
)

type Backoff interface {
	Wait(context.Context, int) error
}

type Router struct {
	mu     sync.RWMutex
	claims *utility.RouteClaims
	routes map[string]vessel.Route
}

func NewRouter() *Router {
	return &Router{claims: utility.NewRouteClaims(), routes: map[string]vessel.Route{}}
}

func (r *Router) reserve(owner string, route vessel.Route) bool {
	segments := make([]string, 0, len(route.Segments))
	for _, segment := range route.Segments {
		segments = append(segments, segment.ID)
	}
	claim := utility.RouteClaim{RouteID: owner, Owner: owner, Segments: segments, CreatedAt: time.Now()}
	if err := r.claims.Reserve(claim); err != nil {
		return false
	}
	r.mu.Lock()
	r.routes[owner] = route.Clone()
	r.mu.Unlock()
	return true
}

func (r *Router) release(owner string) {
	r.claims.Release(owner, owner)
	r.mu.Lock()
	delete(r.routes, owner)
	r.mu.Unlock()
}

func (r *Router) Active() map[string]vessel.Route {
	claims := r.claims.Snapshot()
	r.mu.RLock()
	defer r.mu.RUnlock()
	values := make(map[string]vessel.Route, len(claims))
	for _, claim := range claims {
		values[claim.Owner] = r.routes[claim.Owner].Clone()
	}
	return values
}
