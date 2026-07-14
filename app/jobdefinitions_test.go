package app

import "testing"

const minimalPackageAndWorkflows = `
packages:
  - id: foo
    type: GIT
    url: https://example.com/foo.git

workflows:
  - id: nba-transform
    params:
      - name: foo
        type: string
    steps:
      - action: pkg:find_any
        pkg: foo
        path: "*.zip"
  - id: nba-transaction
    steps:
      - action: pkg:find_any
        pkg: foo
        path: "*.zip"
`

func TestLoadSettings_ParsesJobDefinitions(t *testing.T) {
	path := writeConfig(t, minimalPackageAndWorkflows+`
jobDefinitions:
  - id: nba-pipeline
    parallel: false
    steps:
      - id: nba-transformation
        workflow: nba-transform
        outputs:
          foo: ${outputs.0.foo}
      - id: nba-transaction-step
        workflow: nba-transaction
        parameters:
          foo: ${parent.outputs.foo}
        outputs:
          bar: ${outputs.0.bar}
`)

	settings, err := LoadSettings(path)
	if err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}
	if len(settings.JobDefinitions) != 1 {
		t.Fatalf("len(JobDefinitions) = %d, want 1", len(settings.JobDefinitions))
	}

	def := settings.JobDefinitions[0]
	if def.Id != "nba-pipeline" {
		t.Errorf("Id = %q", def.Id)
	}
	if def.IsParallel() {
		t.Error("expected IsParallel() to be false (explicitly set)")
	}
	if len(def.Steps) != 2 {
		t.Fatalf("len(Steps) = %d, want 2", len(def.Steps))
	}
	if def.Steps[0].Workflow != "nba-transform" {
		t.Errorf("Steps[0].Workflow = %q", def.Steps[0].Workflow)
	}
	if def.Steps[1].Parameters["foo"] != "${parent.outputs.foo}" {
		t.Errorf("Steps[1].Parameters[foo] = %v", def.Steps[1].Parameters["foo"])
	}
}

func TestLoadSettings_JobDefinitionParallelDefaultsToTrue(t *testing.T) {
	path := writeConfig(t, minimalPackageAndWorkflows+`
jobDefinitions:
  - id: nba-pipeline
    steps:
      - id: nba-transformation
        workflow: nba-transform
`)
	settings, err := LoadSettings(path)
	if err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}
	if !settings.JobDefinitions[0].IsParallel() {
		t.Error("expected IsParallel() to default to true")
	}
}

func TestLoadSettings_RejectsDuplicateJobDefinitionId(t *testing.T) {
	path := writeConfig(t, minimalPackageAndWorkflows+`
jobDefinitions:
  - id: nba-pipeline
    steps:
      - id: step-a
        workflow: nba-transform
  - id: nba-pipeline
    steps:
      - id: step-b
        workflow: nba-transform
`)
	if _, err := LoadSettings(path); err == nil {
		t.Fatal("expected an error for a duplicate jobDefinitions id")
	}
}

func TestLoadSettings_RejectsDuplicateStepIdAcrossJobDefinitions(t *testing.T) {
	path := writeConfig(t, minimalPackageAndWorkflows+`
jobDefinitions:
  - id: pipeline-a
    steps:
      - id: shared-step
        workflow: nba-transform
  - id: pipeline-b
    steps:
      - id: shared-step
        workflow: nba-transaction
`)
	if _, err := LoadSettings(path); err == nil {
		t.Fatal("expected an error for a step id reused across different jobDefinitions")
	}
}

func TestLoadSettings_RejectsJobDefinitionWithNoSteps(t *testing.T) {
	path := writeConfig(t, minimalPackageAndWorkflows+`
jobDefinitions:
  - id: nba-pipeline
    steps: []
`)
	if _, err := LoadSettings(path); err == nil {
		t.Fatal("expected an error for a jobDefinition with no steps")
	}
}

func TestLoadSettings_RejectsStepWithoutWorkflow(t *testing.T) {
	path := writeConfig(t, minimalPackageAndWorkflows+`
jobDefinitions:
  - id: nba-pipeline
    steps:
      - id: nba-transformation
`)
	if _, err := LoadSettings(path); err == nil {
		t.Fatal("expected an error for a step without a workflow")
	}
}

func TestLoadSettings_RejectsStepReferencingUnknownWorkflow(t *testing.T) {
	path := writeConfig(t, minimalPackageAndWorkflows+`
jobDefinitions:
  - id: nba-pipeline
    steps:
      - id: nba-transformation
        workflow: does-not-exist
`)
	if _, err := LoadSettings(path); err == nil {
		t.Fatal("expected an error for a step referencing an unknown workflow")
	}
}

func TestSettings_GetJobDefinitionAndGetJobStep(t *testing.T) {
	path := writeConfig(t, minimalPackageAndWorkflows+`
jobDefinitions:
  - id: nba-pipeline
    steps:
      - id: nba-transformation
        workflow: nba-transform
      - id: nba-transaction-step
        workflow: nba-transaction
`)
	settings, err := LoadSettings(path)
	if err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}

	def, err := settings.GetJobDefinition("nba-pipeline")
	if err != nil {
		t.Fatalf("GetJobDefinition: %v", err)
	}
	if def.Id != "nba-pipeline" {
		t.Errorf("GetJobDefinition returned %+v", def)
	}
	if _, err := settings.GetJobDefinition("missing"); err == nil {
		t.Fatal("expected an error for an unknown job definition id")
	}

	owner, step, err := settings.GetJobStep("nba-transaction-step")
	if err != nil {
		t.Fatalf("GetJobStep: %v", err)
	}
	if owner.Id != "nba-pipeline" || step.Workflow != "nba-transaction" {
		t.Errorf("GetJobStep returned owner=%+v step=%+v", owner, step)
	}
	if _, _, err := settings.GetJobStep("missing"); err == nil {
		t.Fatal("expected an error for an unknown job step id")
	}
}
