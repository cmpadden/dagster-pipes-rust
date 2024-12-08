import shutil

from dagster import (
    asset,
    AssetCheckSpec,
    AssetExecutionContext,
    file_relative_path,
    materialize_to_memory,
    MaterializeResult,
    PipesSubprocessClient,
    PipesEnvContextInjector,
)


def test_example_rust_subprocess_asset():
    @asset(
        check_specs=[
            AssetCheckSpec(
                name="example_rust_subprocess_check",
                asset="example_rust_subprocess_asset",
            )
        ],
    )
    def example_rust_subprocess_asset(
        context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
    ) -> MaterializeResult:
        """Demonstrates running Rust binary in a subprocess."""
        cmd = [shutil.which("cargo"), "run"]
        cwd = file_relative_path(__file__, "../rust_processing_jobs")
        return pipes_subprocess_client.run(
            command=cmd,
            cwd=cwd,
            context=context,
        ).get_materialize_result()

    result = materialize_to_memory(
        assets=[example_rust_subprocess_asset],
        resources={
            "pipes_subprocess_client": PipesSubprocessClient(
                context_injector=PipesEnvContextInjector(),
            )
        },
    )
    assert result.success
    assert result.get_asset_materialization_events()[0].asset_key.path == [
        "example_rust_subprocess_asset"
    ]
    assert (
        result.get_asset_check_evaluations()[0].asset_check_key.name
        == "example_rust_subprocess_check"
    )
