package harness

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

type planTestComponent struct { name string; g *DependencyGraph; log *[]string }
func (c *planTestComponent) Name() string { return c.name }
func (c *planTestComponent) Inject() []string { return nil }
func (c *planTestComponent) OnWakeUp(*SpatioTemporalContext) { if c.g != nil { _ = c.g.Generation() }; if c.log != nil { *c.log = append(*c.log, "wake "+c.name) } }
func (c *planTestComponent) OnSleep() { if c.g != nil { _ = c.g.Generation() }; if c.log != nil { *c.log = append(*c.log, "sleep "+c.name) } }

func newPlanGraph(t *testing.T, names ...string) (*DependencyGraph, map[string]*planTestComponent) {
	t.Helper(); g := NewDependencyGraph(); cs := make(map[string]*planTestComponent, len(names))
	for _, name := range names { c := &planTestComponent{name:name}; cs[name]=c; if err:=g.RegisterNode(name,c,nil); err!=nil { t.Fatal(err) } }
	for _, c := range cs { c.g=g }; return g,cs
}
func addPlanEdge(t *testing.T,g *DependencyGraph,p,d string){ t.Helper(); if err:=g.RegisterDependency(p,d); err!=nil { t.Fatal(err) } }
func planNames(p DependencyPlan) []string { out:=make([]string,0,len(p.Actions)); for _,a:=range p.Actions { out=append(out,fmt.Sprintf("%s:%d",a.NodeName,a.Action)) }; return out }

func TestDependencyPlanWakeChain(t *testing.T){
	g,_:=newPlanGraph(t,"A","B","C"); addPlanEdge(t,g,"A","B"); addPlanEdge(t,g,"B","C")
	p,err:=g.PlanWake("A"); if err!=nil { t.Fatal(err) }; if got:=fmt.Sprint(planNames(p)); got!="[A:1 B:1 C:1]" { t.Fatalf("actions=%s",got) }
	if err:=g.Execute(p);err!=nil{t.Fatal(err)}
}

func TestDependencyPlanSleepChain(t *testing.T){
	g,_:=newPlanGraph(t,"A","B","C"); addPlanEdge(t,g,"A","B"); addPlanEdge(t,g,"B","C")
	if _,err:=g.PlanWake("A");err!=nil{t.Fatal(err)}
	p,err:=g.PlanSleep("A");if err!=nil{t.Fatal(err)}
	if got:=fmt.Sprint(planNames(p));got!="[C:2 B:2 A:2]"{t.Fatalf("actions=%s",got)}
	if info,_:=g.Node("A");info.Active{t.Fatal("A remained active")}
}

func TestDependencyPlanMultiParentEligibility(t *testing.T){
	g,_:=newPlanGraph(t,"A","B","D");addPlanEdge(t,g,"A","D");addPlanEdge(t,g,"B","D")
	p,err:=g.PlanWake("A");if err!=nil{t.Fatal(err)};if got:=fmt.Sprint(planNames(p));got!="[A:1]"{t.Fatalf("actions=%s",got)}
	p,err=g.PlanWake("B");if err!=nil{t.Fatal(err)};if got:=fmt.Sprint(planNames(p));got!="[B:1 D:1]"{t.Fatalf("actions=%s",got)}
}

func TestDependencyPlanDiamond(t *testing.T){
	g,_:=newPlanGraph(t,"A","B","C","D");addPlanEdge(t,g,"A","B");addPlanEdge(t,g,"A","C");addPlanEdge(t,g,"B","D");addPlanEdge(t,g,"C","D")
	p,err:=g.PlanWake("A");if err!=nil{t.Fatal(err)};if got:=fmt.Sprint(planNames(p));got!="[A:1 B:1 C:1 D:1]"{t.Fatalf("wake=%s",got)}
	p,err=g.PlanSleep("A");if err!=nil{t.Fatal(err)};if got:=fmt.Sprint(planNames(p));got!="[D:2 C:2 B:2 A:2]"{t.Fatalf("sleep=%s",got)}
	count:=0;for _,a:=range p.Actions{if a.NodeName=="D"{count++}};if count!=1{t.Fatalf("D action count=%d",count)}
}

func TestDependencyPlanGenerationAndStalePlan(t *testing.T){
	g,_:=newPlanGraph(t,"A");before:=g.Generation();p,err:=g.PlanWake("A");if err!=nil{t.Fatal(err)};if p.Generation<=before{t.Fatalf("generation did not advance")}
	if err:=g.SetNodeActive("A",false);err!=nil{t.Fatal(err)};if err:=g.Execute(p);!errors.Is(err,ErrDependencyPlanStale){t.Fatalf("err=%v",err)}
}

func TestDependencyPlanCallbacksAndContextBoundary(t *testing.T){
	g,cs:=newPlanGraph(t,"A","B");addPlanEdge(t,g,"A","B");log:=make([]string,0,2);cs["A"].log=&log;cs["B"].log=&log
	p,err:=g.PlanWake("A");if err!=nil{t.Fatal(err)};if err:=g.Execute(p);err!=nil{t.Fatal(err)};if fmt.Sprint(log)!="[wake A wake B]"{t.Fatalf("log=%v",log)}
}

func TestDependencyPlanConcurrentPlanning(t *testing.T){
	g,_:=newPlanGraph(t,"A","B","C");addPlanEdge(t,g,"A","B");addPlanEdge(t,g,"B","C");var wg sync.WaitGroup
	for i:=0;i<32;i++{wg.Add(2);go func(){defer wg.Done();for j:=0;j<50;j++{_,_=g.PlanWake("A")}}();go func(){defer wg.Done();for j:=0;j<50;j++{_,_=g.PlanSleep("A")}}()};wg.Wait();if err:=g.Validate();err!=nil{t.Fatal(err)}
}
