# Docker Configs
Two simple containers to run the reindexer, assembled by [docker compose](https://docs.docker.com/compose/)

- reindeer_x (reindexer)
    - Job Management management api
    - [Bee Queue](https://github.com/bee-queue/bee-queue): fast lightweight job queue
    - [Arena](https://github.com/bee-queue/arena): Bull/Bee queue status UI
- redis
    - local job store for [Bee Queue](https://github.com/bee-queue/bee-queue)



