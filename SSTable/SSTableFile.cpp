#include "SSTableFile.h"
#include "fmt/format.h"

SSTableFile::SSTableFile(const std::filesystem::path &directory, int index, MemCache &memcache,
                         const std::unordered_set<std::string>& tombstones) : index(index) {

    auto filename = fmt::format(fmt::runtime(ssTableFilenameFormat), index);
    file.open(directory / filename, std::ios::in | std::ios::trunc);
}

SSTableFile::SSTableFile(const std::filesystem::path &filepath) {
    if (!isFilenameSSTable(filepath)){
        throw std::runtime_error("File " + filepath.string() + " is not a valid SSTable file");
    }

    index = SSTableFile::extractIndexFromFilename(filepath).value();
    file.open(filepath);
}

void SSTableFile::writeToFile(MemCache &memcache, const std::unordered_set<std::string>& tombstones) {

}

std::optional<int> SSTableFile::extractIndexFromFilename(const std::filesystem::path &filename) {

}

bool SSTableFile::isFilenameSSTable(const std::filesystem::path &filename) {
    return false;
}



