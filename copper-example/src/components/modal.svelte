<script>
  import { fade } from "svelte/transition";
  export let visible = false;
</script>

{#if visible}
  <div
    class="absolute flex justify-center items-start w-full h-full bg-black/90"
    transition:fade={{ duration: 200 }}
  >
    <div class="modal">
      <slot />
    </div>
  </div>
{/if}
