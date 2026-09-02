package app

import "testing"

const minimalPackageAndWorkflows = `
packages:
  - id: foo
    type: GIT
    url: https://example.com/foo.git

workflows:
  - id: nba-transform
    parameters:
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
jobs:
  - kind: nba-transformation
    workflow: nba-transform
    outputs:
      foo: ${outputs.0.foo}
  - kind: nba-transaction-step
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
	if len(settings.JobDefinitions) != 2 {
		t.Fatalf("len(JobDefinitions) = %d, want 2", len(settings.JobDefinitions))
	}

	if settings.JobDefinitions[0].Kind != "nba-transformation" {
		t.Errorf("[0].Kind = %q", settings.JobDefinitions[0].Kind)
	}
	if settings.JobDefinitions[0].Workflow != "nba-transform" {
		t.Errorf("[0].Workflow = %q", settings.JobDefinitions[0].Workflow)
	}
	if settings.JobDefinitions[1].Parameters["foo"] != "${parent.outputs.foo}" {
		t.Errorf("[1].Parameters[foo] = %v", settings.JobDefinitions[1].Parameters["foo"])
	}
}

func TestLoadSettings_RejectsDuplicateJobDefinitionId(t *testing.T) {
	path := writeConfig(t, minimalPackageAndWorkflows+`
jobs:
  - kind: shared-id
    workflow: nba-transform
  - kind: shared-id
    workflow: nba-transaction
`)
	if _, err := LoadSettings(path); err == nil {
		t.Fatal("expected an error for a duplicate jobs id")
	}
}

func TestLoadSettings_RejectsDefinitionWithoutWorkflow(t *testing.T) {
	path := writeConfig(t, minimalPackageAndWorkflows+`
jobs:
  - kind: nba-transformation
`)
	if _, err := LoadSettings(path); err == nil {
		t.Fatal("expected an error for a jobDefinition without a workflow")
	}
}

func TestLoadSettings_RejectsDefinitionReferencingUnknownWorkflow(t *testing.T) {
	path := writeConfig(t, minimalPackageAndWorkflows+`
jobs:
  - kind: nba-transformation
    workflow: does-not-exist
`)
	if _, err := LoadSettings(path); err == nil {
		t.Fatal("expected an error for a jobDefinition referencing an unknown workflow")
	}
}

func TestSettings_GetJobDefinition(t *testing.T) {
	path := writeConfig(t, minimalPackageAndWorkflows+`
jobs:
  - kind: nba-transformation
    workflow: nba-transform
  - kind: nba-transaction-step
    workflow: nba-transaction
`)
	settings, err := LoadSettings(path)
	if err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}

	def, err := settings.GetJobDefinition("nba-transaction-step")
	if err != nil {
		t.Fatalf("GetJobDefinition: %v", err)
	}
	if def.Kind != "nba-transaction-step" || def.Workflow != "nba-transaction" {
		t.Errorf("GetJobDefinition returned %+v", def)
	}
	if _, err := settings.GetJobDefinition("missing"); err == nil {
		t.Fatal("expected an error for an unknown job definition id")
	}
}
