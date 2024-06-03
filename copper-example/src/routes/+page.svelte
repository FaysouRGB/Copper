<script>
  // @ts-nocheck

  import { invoke } from "@tauri-apps/api/tauri";
  import Modal from "../components/modal.svelte";
  import { slide } from "svelte/transition";

  let libraries = [];
  let isLoading = false;
  let showInfo = false;
  let showNew = false;

  let newLibraryName = "";

  async function fetchLibraries() {
    libraries = await invoke("get_shops");
    libraries = libraries.sort();
  }

  async function createLibrary() {
    if (isLoading) return;
    isLoading = true;
    await invoke("create_shop", { name: newLibraryName });
    window.location.assign("/manager?name=" + newLibraryName);
  }

  async function deleteLibrary(name) {
    if (isLoading) return;
    isLoading = true;
    await invoke("delete_shop", { name });
    await fetchLibraries();
    isLoading = false;
  }

  fetchLibraries();
</script>

<div class="h-full w-full flex flex-col">
  <!--About modal-->
  <Modal bind:visible={showInfo}>
    <h1>About</h1>
    <hr />
    <p>
      This library manager is an example software created to showcase the use of
      COPPER DBMS, a database manager system written in Rust.
    </p>
    <table>
      <tr>
        <th class="w-1/4">Name</th>
        <th>Description</th>
      </tr>
      <tr>
        <td
          ><div class="flex items-center">
            <img src="/copper.png" alt="" class="h-8 w-8 inline mr-2" />Copper
          </div></td
        >
        <td>Database manager written in Rust, supports some SQL Queries.</td>
      </tr>
      <tr>
        <td
          ><div class="flex items-center">
            <img src="/svelte.svg" alt="" class="h-8 w-8 inline mr-2" />Svelte
          </div></td
        >
        <td
          >Interface framework to build apps declaratively out of components.</td
        >
      </tr>
      <tr>
        <td
          ><div class="flex items-center">
            <img src="/tauri.svg" alt="" class="h-8 w-8 inline mr-2" />Tauri
          </div></td
        >
        <td
          >Create cross-platform apps using frontend stack for the interface and
          Rust as a backend.</td
        >
      </tr>
      <tr>
        <td
          ><div class="flex items-center">
            <img
              src="/tailwind.svg"
              alt=""
              class="h-8 w-8 inline mr-2"
            />Tailwind CSS
          </div></td
        >
        <td
          >A utility-first CSS framework packed with classes that can be
          composed to build any design directly in your markup.</td
        >
      </tr>
    </table>
    <p>
      This project has been developed by Fayçal Beghalia, Wilfrid Wangon-Zekou
      and Salma Boubakkar.
    </p>
    <button class="neutral-button float-end" on:click={() => (showInfo = false)}
      >Close</button
    >
  </Modal>

  <!--New library modal-->
  <Modal bind:visible={showNew}>
    <h1>Create a library</h1>
    <input type="text" bind:value={newLibraryName} placeholder="Library name" />
    <div class="flex space-x-4 justify-end">
      <button class="neutral-button" on:click={() => (showNew = false)}
        >Close</button
      >
      <button
        class="green-button"
        disabled={isLoading}
        on:click={() => createLibrary()}>Create</button
      >
    </div>
  </Modal>

  <!--Top menu-->
  <div class="menu">
    <div class="flex items-center">
      <img
        src="/blue_book_3d.png"
        alt="Blue Book"
        class="h-8 w-8 inline mr-2"
      />
      <h1>Library Manager</h1>
    </div>
    <button
      on:click={() => {
        newLibraryName = "";
        showNew = true;
      }}
      class="green-button">New library</button
    >
  </div>

  <!--List-->
  <div class="flex-1 p-4 space-y-4 bg-[url(/wallpaper.png)] bg-cover">
    {#each libraries as library, index}
      <div
        class="flex justify-between"
        key={index}
        transition:slide={{ duration: 200, delay: index * 50 }}
      >
        <a
          href="manager?name={library}"
          class="flex-1 p-4 transition-colors text-sm shadow-md rounded-l-sm bg-gray-200 hover:bg-gray-400"
          >{library}</a
        >
        <button
          on:click={() => deleteLibrary(library)}
          disabled={isLoading}
          class="red-button !rounded-l-none !rounded-r-sm">Delete</button
        >
      </div>
    {:else}
      <div
        class="h-full w-full flex flex-col space-y-4 justify-center items-center"
      >
        <img src="/eyes_3d.png" alt="Eyes" class="h-32 w-32" />
        <p class="text-white/50">
          No libraries. Create a library using the "New library" button.
        </p>
      </div>
    {/each}
  </div>

  <!--Bottom menu-->
  <div class="menu">
    <span class="italic">© 2024 - Library Manager version 1.0</span>
    <button on:click={() => (showInfo = true)} class="neutral-button"
      >About</button
    >
  </div>
</div>
