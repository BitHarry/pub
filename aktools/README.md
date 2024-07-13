
 **Add the `utils` repository as a submodule:**

   ```sh
   git submodule add ssh://git@git.source.akamai.com:7999/~akleytma/utils.git utils 
   ```

   ```sh
   git submodule init
   ```

   ```sh
   git submodule update
   ```

   ```sh
   git add .
   git commit -m "Added utils submodule"
   git push
   ```
**to clone utils when cloning your repo**

```sh
 git clone --recurse-submodules  <your repo url>
 ```
