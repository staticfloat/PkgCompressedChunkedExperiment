using Downloads

# These are registry hashes available on `pkg.julialang.org`
registry_hashes = [
    "dc2720b2e5ab6c7d8d35bbf5853edc3e5682c5cc", # 2025-03-01
    "c86fd8f3617fa06bffa18b9316cf17baaf3bd904", # 2025-03-15
    "080dd52fa032049906f0b4cba225cd84e000ddd8", # 2025-03-25
    "c4e89c781bd758da0271850aa629c593fa87d93d", # 2025-03-26
]

registries_dir = joinpath(@__DIR__, "registries")
mkpath(registries_dir)

function registry_download_path(hash::String)
    return joinpath(registries_dir, hash)
end

function download_registry(hash::String)
    registry_url = "https://pkg.julialang.org/registry/23338594-aafe-5451-b93e-139f81909106"
    Downloads.download("$(registry_url)/$(hash)", registry_download_path(hash))
end
