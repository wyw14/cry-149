package oxygen

import (
	"math"
	"sync"
	"time"
)

type Mode string

const (
	Automatic Mode = "automatic"
	Manual    Mode = "manual"
	Paused    Mode = "paused"
)

type Output struct {
	FeedRate  float64   `json:"feed_rate"`
	Agitation float64   `json:"agitation"`
	At        time.Time `json:"at"`
}

type Controller struct {
	mu         sync.Mutex
	mode       Mode
	target     float64
	integral   float64
	lastInput  float64
	lastOutput Output
	lastAt     time.Time
}

func NewController(target float64, now time.Time) *Controller {
	return &Controller{mode: Automatic, target: target, lastAt: now}
}

func (c *Controller) Observe(value float64, now time.Time) Output {
	c.mu.Lock()
	defer c.mu.Unlock()
	delta := now.Sub(c.lastAt).Seconds()
	if delta < 0 || delta > 10 {
		delta = 1
	}
	c.lastInput = value
	c.lastAt = now
	if c.mode != Automatic {
		return c.lastOutput
	}
	errorValue := c.target - value
	c.integral = clamp(c.integral+errorValue*delta*0.05, -20, 20)
	feedRate := clamp(errorValue*0.7+c.integral, 0, 100)
	agitation := clamp(35+errorValue*1.8+c.integral*0.6, 20, 100)
	c.lastOutput = Output{FeedRate: feedRate, Agitation: agitation, At: now}
	return c.lastOutput
}

func (c *Controller) SetMode(mode Mode, manual Output, now time.Time) Output {
	c.mu.Lock()
	defer c.mu.Unlock()
	previous := c.mode
	c.mode = mode
	c.lastAt = now
	if mode == Manual {
		c.integral = 0
		manual.At = now
		c.lastOutput = manual
		return c.lastOutput
	}
	if mode == Paused {
		c.integral = 0
		c.lastOutput = Output{At: now}
		return c.lastOutput
	}
	if previous != Automatic {
		errorValue := c.target - c.lastInput
		c.integral = clamp(c.lastOutput.FeedRate-errorValue*0.7, -20, 20)
	}
	return c.lastOutput
}

func (c *Controller) State() (Mode, float64, Output) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mode, c.integral, c.lastOutput
}

func clamp(value, minimum, maximum float64) float64 {
	return math.Min(maximum, math.Max(minimum, value))
}
