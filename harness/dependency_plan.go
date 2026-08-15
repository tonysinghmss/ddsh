package harness

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

type DependencyActionType uint8
const (
	DependencyActionWake DependencyActionType = iota + 1
	DependencyActionSleep
)
var (
	ErrDependencyPlanStale = errors.New("dependency plan is stale")
	ErrDependencyPlanInvalid = errors.New("invalid dependency plan")
)

type DependencyAction struct { NodeName string; Action DependencyActionType; Component Component; Context *SpatioTemporalContext }
type DependencyPlan struct { Generation uint64; Actions []DependencyAction }
type DependencyExecutionOptions struct { ParentContext context.Context; Recovery RecoveryStrategy }

func (g *DependencyGraph) PlanWake(name string) (DependencyPlan,error) {
	g.planMu.Lock(); defer g.planMu.Unlock(); g.mu.Lock(); defer g.mu.Unlock()
	if _,ok:=g.nodes[name];!ok{return DependencyPlan{},fmt.Errorf("%w: %q",ErrDependencyNodeNotFound,name)}
	a,changed,err:=g.planWakeLocked(name);if err!=nil{return DependencyPlan{},err};if changed{g.generation++};return DependencyPlan{Generation:g.generation,Actions:a},nil
}

// PlanSleep includes the root's own sleep action followed by invalidated downstream nodes.
func (g *DependencyGraph) PlanSleep(name string)(DependencyPlan,error){
	g.planMu.Lock();defer g.planMu.Unlock();g.mu.Lock();defer g.mu.Unlock()
	if _,ok:=g.nodes[name];!ok{return DependencyPlan{},fmt.Errorf("%w: %q",ErrDependencyNodeNotFound,name)}
	a,changed,err:=g.planSleepLocked(name);if err!=nil{return DependencyPlan{},err};if changed{g.generation++};return DependencyPlan{Generation:g.generation,Actions:a},nil
}

func (g *DependencyGraph) Execute(plan DependencyPlan) error{return g.ExecuteWithOptions(plan,DependencyExecutionOptions{})}

// ExecuteWithOptions is the single execution boundary for wake and sleep plans.
// planMu serializes transitions; graph.mu is never held while application code runs.
func (g *DependencyGraph) ExecuteWithOptions(plan DependencyPlan,o DependencyExecutionOptions) error{
	if o.ParentContext==nil{o.ParentContext=context.Background()};if err:=validateDependencyPlan(plan);err!=nil{return err}
	g.planMu.Lock();defer g.planMu.Unlock()
	for _,a:=range plan.Actions{
		g.mu.RLock();gen:=g.generation;valid:=gen==plan.Generation&&g.actionMatchesNodeLocked(a);g.mu.RUnlock()
		if !valid{return fmt.Errorf("%w: plan generation=%d graph generation=%d",ErrDependencyPlanStale,plan.Generation,gen)}
		switch a.Action{
		case DependencyActionWake: if err:=g.executeWakeAction(o,a);err!=nil{return err}
		case DependencyActionSleep:
			if a.Component!=nil{a.Component.OnSleep()};if a.Context!=nil{a.Context.Rollback()};g.clearExecutedContext(a)
		}
	}
	return nil
}

func validateDependencyPlan(p DependencyPlan)error{
	seen:=make(map[string]struct{},len(p.Actions));for _,a:=range p.Actions{
		if a.NodeName==""{return fmt.Errorf("%w: missing node name",ErrDependencyPlanInvalid)}
		if a.Action!=DependencyActionWake&&a.Action!=DependencyActionSleep{return fmt.Errorf("%w: unknown action type %d",ErrDependencyPlanInvalid,a.Action)}
		if _,ok:=seen[a.NodeName];ok{return fmt.Errorf("%w: duplicate action for %q",ErrDependencyPlanInvalid,a.NodeName)};seen[a.NodeName]=struct{}{}
	}
	return nil
}

func(g *DependencyGraph)executeWakeAction(o DependencyExecutionOptions,a DependencyAction)error{
	if a.Component==nil{return nil}
	ctx:=a.Context
	if ctx==nil{ctx=NewSTContext(o.ParentContext,a.NodeName,o.Recovery);g.mu.Lock();n,ok:=g.nodes[a.NodeName];if !ok||!n.active||n.component!=a.Component||n.context!=nil{g.mu.Unlock();return fmt.Errorf("%w: wake context materialization changed node %q",ErrDependencyPlanStale,a.NodeName)};n.context=ctx;g.mu.Unlock()}
	a.Component.OnWakeUp(ctx);return nil
}
func(g *DependencyGraph)clearExecutedContext(a DependencyAction){g.mu.Lock();defer g.mu.Unlock();if n,ok:=g.nodes[a.NodeName];ok&&n.context==a.Context{n.context=nil}}
func(g *DependencyGraph)Generation()uint64{g.mu.RLock();defer g.mu.RUnlock();return g.generation}
func(g *DependencyGraph)actionMatchesNodeLocked(a DependencyAction)bool{if a.NodeName==""||(a.Action!=DependencyActionWake&&a.Action!=DependencyActionSleep){return false};n,ok:=g.nodes[a.NodeName];if !ok||n.component!=a.Component{return false};if a.Action==DependencyActionWake{return n.active&&(a.Context==nil||n.context==a.Context)};return !n.active&&n.context==a.Context}

func(g *DependencyGraph)planWakeLocked(name string)([]DependencyAction,bool,error){
	if g.nodes[name].active{return nil,false,nil};cand:=map[string]struct{}{name:{}};q:=[]string{name}
	for len(q)>0{cur:=q[0];q=q[1:];for _,d:=range sortedDependencyNames(g.nodes[cur].dependents){if _,ok:=cand[d];ok{continue};cand[d]=struct{}{};q=append(q,d)}}
	ind:=make(map[string]int,len(cand));for n:=range cand{for p:=range g.nodes[n].providers{if !g.nodes[p].active{ind[n]++}}}
	ready:=make([]string,0,len(cand));for n:=range cand{if ind[n]==0{ready=append(ready,n)}};sort.Strings(ready)
	actions:=make([]DependencyAction,0,len(cand));changed:=false
	for len(ready)>0{cur:=ready[0];ready=ready[1:];n:=g.nodes[cur];if !n.active{n.active=true;changed=true;actions=append(actions,DependencyAction{NodeName:cur,Action:DependencyActionWake,Component:n.component,Context:n.context})};for _,d:=range sortedDependencyNames(n.dependents){if _,ok:=cand[d];!ok{continue};if ind[d]>0{ind[d]--};if ind[d]==0{ready=append(ready,d);sort.Strings(ready)}}}
	return actions,changed,nil
}

func(g *DependencyGraph)planSleepLocked(name string)([]DependencyAction,bool,error){
	if !g.nodes[name].active{return nil,false,nil};cand:=map[string]struct{}{name:{}};q:=[]string{name}
	for len(q)>0{cur:=q[0];q=q[1:];for _,d:=range sortedDependencyNames(g.nodes[cur].dependents){if _,ok:=cand[d];ok{continue};cand[d]=struct{}{};q=append(q,d)}}
	invalid:=map[string]struct{}{name:{}};ordered,err:=g.topologicalSubsetLocked(cand);if err!=nil{return nil,false,err}
	for _,n:=range ordered{if n==name{continue};node:=g.nodes[n];if !node.active{continue};valid:=false;for p:=range node.providers{if _,bad:=invalid[p];bad{continue};if g.nodes[p].active{valid=true;break}};if len(node.providers)==0{valid=true};if !valid{invalid[n]=struct{}{}}}
	actions:=make([]DependencyAction,0,len(invalid));for i:=len(ordered)-1;i>=0;i--{n:=ordered[i];if _,bad:=invalid[n];!bad{continue};node:=g.nodes[n];if !node.active{continue};node.active=false;actions=append(actions,DependencyAction{NodeName:n,Action:DependencyActionSleep,Component:node.component,Context:node.context})}
	return actions,true,nil
}

func(g *DependencyGraph)topologicalSubsetLocked(sub map[string]struct{})([]string,error){ind:=make(map[string]int,len(sub));for n:=range sub{for p:=range g.nodes[n].providers{if _,ok:=sub[p];ok{ind[n]++}}};ready:=make([]string,0,len(sub));for n:=range sub{if ind[n]==0{ready=append(ready,n)}};sort.Strings(ready);order:=make([]string,0,len(sub));for len(ready)>0{cur:=ready[0];ready=ready[1:];order=append(order,cur);for _,d:=range sortedDependencyNames(g.nodes[cur].dependents){if _,ok:=sub[d];!ok{continue};ind[d]--;if ind[d]==0{ready=append(ready,d);sort.Strings(ready)}}};if len(order)!=len(sub){return nil,ErrDependencyCycle};return order,nil}
func sortedDependencyNames(edges map[string]*dependencyEdge)[]string{out:=make([]string,0,len(edges));for n:=range edges{out=append(out,n)};sort.Strings(out);return out}
