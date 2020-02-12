class TestScriptPackage extends GrokPackage {

    // Guaranteed to get called exactly once before the execution of any function below
    init() { console.log('Test Script package initialized.'); }
}
